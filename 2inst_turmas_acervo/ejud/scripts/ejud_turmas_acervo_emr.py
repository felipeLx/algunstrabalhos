#!/usr/bin/env python3
"""
EJUD Turmas Acervo — EMR Iceberg writer.

Source:  glue_catalog.ejud.* (bronze Parquet tables, Glue Data Catalog)
Target:  glue_catalog.dw_2inst_ouro.tb_ft_turmas_acervo
           partitioned by dth_referencia (DATE)

Logic mirrors acervo_turmas.py + turmas_staged_builder.py, adapted for
Glue bronze tables instead of local parquet.

Usage:
  spark-submit \\
    --master yarn --deploy-mode client \\
    --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
    s3://sgdai-projetos/2instancia/2inst_turmas_acervo/ejud/scripts/ejud_turmas_acervo_emr.py \\
    --dateref 2026-04-24
"""

import argparse
import logging
import sys
from datetime import datetime, date
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, col, regexp_replace
from pyspark.storagelevel import StorageLevel

# ── Constants ─────────────────────────────────────────────────────────────────
_CATALOG        = "glue_catalog"
_WAREHOUSE      = "s3://sgdai-data-lake"
_TARGET_SCHEMA  = "dw_2inst_ouro"
_TARGET_TABLE   = "tb_ft_turmas_acervo"
_PARTITION_COLS = ["dth_referencia"]
_SL             = StorageLevel.MEMORY_AND_DISK

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_turmas_acervo_emr")


# ── SparkSession ──────────────────────────────────────────────────────────────

def _create_spark_session(
    app_name: str = "EJUD_Turmas_Acervo",
    executor_memory: str = "18g",
    executor_memory_overhead: str = "4g",
    shuffle_partitions: int = 64,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"{_WAREHOUSE}/ouro")
        .config("spark.sql.defaultCatalog", _CATALOG)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


# ── Iceberg helpers ───────────────────────────────────────────────────────────

def _ensure_iceberg_table(
    spark: SparkSession,
    schema: str,
    table: str,
    df: DataFrame,
    partition_cols: List[str],
) -> None:
    fqn      = f"{_CATALOG}.{schema}.{table}"
    location = f"{_WAREHOUSE}/ouro/{schema}/{table}/"
    type_map = {
        "string": "STRING", "long": "BIGINT", "integer": "INT",
        "double": "DOUBLE", "float": "FLOAT", "boolean": "BOOLEAN",
        "date": "DATE", "timestamp": "TIMESTAMP", "timestamp_ntz": "TIMESTAMP",
    }
    col_defs = [
        f"  `{f.name}` {type_map.get(str(f.dataType.typeName()).lower(), 'STRING')}"
        for f in df.schema.fields
    ]
    non_part = [d for d in col_defs
                if not any(d.strip().startswith(f"`{p}`") for p in partition_cols)]
    part_def = [d for d in col_defs
                if any(d.strip().startswith(f"`{p}`") for p in partition_cols)]
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        + ",\n".join(non_part + part_def)
        + f"\n)\nUSING iceberg\nPARTITIONED BY ({', '.join(partition_cols)})\n"
        + f"LOCATION '{location}'\n"
        + "TBLPROPERTIES (\n"
        + "  'format-version' = '2',\n"
        + "  'write.format.default' = 'parquet',\n"
        + "  'write.parquet.compression-codec' = 'snappy'\n)"
    )
    logger.info("Ensuring Iceberg table: %s", fqn)
    spark.sql(ddl)


def _save_to_iceberg(
    df: DataFrame,
    schema: str = _TARGET_SCHEMA,
    table: str = _TARGET_TABLE,
) -> None:
    fqn = f"{_CATALOG}.{schema}.{table}"
    logger.info("Saving to Iceberg: %s", fqn)
    _ensure_iceberg_table(df.sparkSession, schema, table, df, _PARTITION_COLS)
    df.writeTo(fqn).option("write-audit-publish", "false").overwritePartitions()
    logger.info("Iceberg write complete: %s", fqn)


# ── Calculator ────────────────────────────────────────────────────────────────

class TurmasAcervoCalculator:
    """
    EJUD Turmas Acervo.

    Eligibility: t_processo (codcompt IN turmas competencias, codtipproc != 288)
    filtered by last movimentorelator.codorgjulg IN turmas OJs.
    movimentofisico is a reporting-only LEFT JOIN (no codlocal filter).

    Reads from Glue bronze (glue_catalog.ejud.*), runs staged SQL
    matching turmas_staged_builder.py logic, writes to Iceberg ouro.

    Output: 95 columns — same 87 as turmas_relatorio_final (PG) plus
            8 ind_excl_* / ind_acervo_liquido flags for analytics.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = _CATALOG,
        db_bronze: str = "ejud",
        db_ouro: str = "dw_2inst_ouro",
    ):
        self.spark    = spark
        self.catalog  = catalog
        self.db_bronze = db_bronze
        self.db_ouro  = db_ouro

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _tbl(self, db: str, table: str) -> str:
        return f"{self.catalog}.{db}.{table}"

    @staticmethod
    def _norm(column) -> "Column":
        """Strip .0 / scientific notation from numeric-as-string coddoc."""
        return regexp_replace(
            regexp_replace(column.cast("string"), r"\.0+$", ""),
            r"^(\d+)\.(\d+)E(\d+)$", "$1$2",
        )

    def _load(self, table: str, norm_coddoc: bool = True) -> DataFrame:
        """Load a Glue bronze table, optionally normalize coddoc."""
        df = self.spark.table(self._tbl(self.db_bronze, table))
        if norm_coddoc and "coddoc" in df.columns:
            df = df.withColumn("coddoc", self._norm(col("coddoc")))
        return df

    def _mat(self, name: str, sql: str, v: str) -> DataFrame:
        """Execute SQL, persist, count, register TempView."""
        df = self.spark.sql(sql.format(v=v))
        df.persist(_SL)
        n = df.count()
        df.createOrReplaceTempView(name)
        logger.info("  [%s] %s rows", name, f"{n:,}")
        return df

    # ── Bronze table loading ──────────────────────────────────────────────────

    # Turmas OJs — codorgjulg das turmas do TJRJ 2a instância
    _TURMAS_ORGAOS = (681, 682, 3553, 3798, 3799, 3800, 6676, 6739, 6740, 9525, 9816, 9940)

    def load_bronze_tables(self, v_dateref_str: str) -> None:
        """
        Load all source tables from Glue bronze, register as TempViews.

        Eligibility filter (director-approved):
          1. t_processo.codcompt IN (turmas competencias) AND codtipproc != 288
          2. last movimentorelator.codorgjulg IN (turmas OJs)

        movimentofisico is NOT a filter — registered as TempView for
        reporting (local_fisico_stg / excl_local_saida) only.
        """
        logger.info("Loading dimension tables from Glue bronze...")

        # Small dimension tables — register directly (no coddoc needed for most)
        dims_no_coddoc = [
            "magistrado", "orgaojulgador", "localvirtual",
            "assunto", "vara", "fase", "faseexterna", "complemento",
            "tipoprocesso", "localfisico", "tipoparte", "competencia",
        ]
        for t in dims_no_coddoc:
            self.spark.table(self._tbl(self.db_bronze, t)).createOrReplaceTempView(t)

        # Dimension tables that have coddoc
        dims_with_coddoc = [
            "logprocesso", "processoassunto", "autoassociado",
            "parte", "parteprocesso", "localvirtualdocumento", "movsobrestamento",
            "pessoa", "movdistribuicao", "movcancedistr",
        ]
        for t in dims_with_coddoc:
            self._load(t).createOrReplaceTempView(t)

        # ── Pre-stage 1: proc_base — t_processo with codcompt/codtipproc filters
        logger.info("Pre-stage 1: proc_base (t_processo codcompt/codtipproc filter)...")
        self._load("t_processo").createOrReplaceTempView("t_processo")
        df_proc_base = self.spark.sql("""
            SELECT coddoc, codtipproc, codcompt
            FROM t_processo
            WHERE codtipproc != 288
              AND codcompt IN (10, 11, 12, 14, 15, 19, 20, 22)
        """)
        df_proc_base.persist(_SL)
        qt_base = df_proc_base.count()
        df_proc_base.createOrReplaceTempView("proc_base_stg")
        logger.info("  proc_base_stg: %s processos", f"{qt_base:,}")

        # ── Pre-stage 2: last codorgjulg per coddoc from movimentorelator ─────
        # Filter movimentorelator to proc_base scope first to avoid full scan.
        logger.info("Pre-stage 2: last_orgaojulg_stg (movimentorelator filtered)...")
        proc_base_coddocs = df_proc_base.select("coddoc")
        df_mr_base = (
            self._load("movimentorelator")
            .join(broadcast(proc_base_coddocs), on="coddoc", how="inner")
            .filter(f"dthrmov <= '{v_dateref_str}'")
        )
        df_mr_base.persist(_SL)
        df_mr_base.count()
        df_mr_base.createOrReplaceTempView("movimentorelator_base_stg")

        df_last_oj = self.spark.sql("""
            SELECT coddoc, codorgjulg AS last_codorgjulg
            FROM (
                SELECT mr.coddoc, mr.codorgjulg,
                       ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov DESC) AS rn
                FROM movimentorelator_base_stg mr
                JOIN orgaojulgador oj  ON mr.codorgjulg = oj.codorgjulg
                JOIN competencia   com ON oj.codcompt   = com.codcompt
                WHERE com.indconsrec = 'S'
            ) t WHERE rn = 1
        """)
        df_last_oj.persist(_SL)
        qt_oj = df_last_oj.count()
        df_last_oj.createOrReplaceTempView("last_orgaojulg_stg")
        logger.info("  last_orgaojulg_stg: %s coddocs com movimentorelator", f"{qt_oj:,}")

        # ── Pre-stage 3: proc_atual — proc_base filtered by turmas OJs ────────
        logger.info("Pre-stage 3: proc_atual (last codorgjulg IN turmas OJs)...")
        turmas_oj_list = ", ".join(str(x) for x in self._TURMAS_ORGAOS)
        df_proc = self.spark.sql(f"""
            SELECT pb.coddoc, pb.codtipproc, pb.codcompt
            FROM proc_base_stg pb
            JOIN last_orgaojulg_stg oj ON pb.coddoc = oj.coddoc
            WHERE oj.last_codorgjulg IN ({turmas_oj_list})
        """)
        df_proc.persist(_SL)
        qt_proc = df_proc.count()
        df_proc.createOrReplaceTempView("proc_atual_stg")
        logger.info("  proc_atual_stg: %s processos elegiveis", f"{qt_proc:,}")

        # ── Pre-filter heavy tables to proc_atual scope ────────────────────────
        proc_coddocs = df_proc.select("coddoc")

        # movimento: needed for staged SQL steps 4-14
        df_mov = (self._load("movimento")
                  .join(broadcast(proc_coddocs), on="coddoc", how="inner"))
        df_mov.persist(_SL)
        df_mov.count()
        df_mov.createOrReplaceTempView("movimento")
        logger.info("  movimento: pre-filtered to proc_atual_stg scope.")

        # movimentofisico: no codlocal filter — LEFT JOIN for reporting only
        df_mf = (self._load("movimentofisico")
                 .join(broadcast(proc_coddocs), on="coddoc", how="inner"))
        df_mf.persist(_SL)
        df_mf.count()
        df_mf.createOrReplaceTempView("movimentofisico")
        logger.info("  movimentofisico: filtered to proc_atual_stg scope (no codlocal filter).")

        # movimentorelator: re-filter mr_base to proc_atual (smaller scope)
        (df_mr_base.join(broadcast(proc_coddocs), on="coddoc", how="inner")
                   .createOrReplaceTempView("movimentorelator"))

    # ── Stage 1: turmas_base ──────────────────────────────────────────────────

    def build_turmas_base(self, v: str) -> int:
        """
        Base acervo: proc_atual_stg (codcompt turmas + last codorgjulg IN turmas OJs).
        ultima_distribuicao (codfase 26/36) is LEFT JOIN — processes without a
        distribution event are still eligible (dataultdis = NULL).
        movimentofisico is NOT a filter — reporting-only LEFT JOIN in staged SQL.
        """
        logger.info("Stage 1: building turmas_base...")
        sql_base = f"""
            WITH
            dist_cancelada AS (
                SELECT coddoc, dthrmovdistrcance
                FROM movcancedistr
            ),
            ultima_distribuicao AS (
                SELECT md.coddoc, MAX(md.dthrmov) AS dataultdis
                FROM movdistribuicao md
                LEFT JOIN dist_cancelada dc
                       ON md.coddoc = dc.coddoc AND md.dthrmov = dc.dthrmovdistrcance
                WHERE md.codfase IN (26, 36, 60012)
                  AND md.dthrmov <= '{v}'
                  AND dc.coddoc IS NULL
                GROUP BY md.coddoc
            )
            SELECT pa.coddoc,
                   ud.dataultdis,
                   '{v}' AS datareferencia
            FROM proc_atual_stg pa
            LEFT JOIN ultima_distribuicao ud ON pa.coddoc = ud.coddoc
        """
        df_base = self.spark.sql(sql_base)
        df_base.persist(_SL)
        df_base.createOrReplaceTempView("turmas_base")
        qt = df_base.count()
        logger.info("  turmas_base: %s processos elegiveis", f"{qt:,}")

        # Re-filter heavy tables to eligible coddocs only
        coddocs_df = df_base.select("coddoc")
        for tbl in ["movimento", "movimentorelator"]:
            df_filt = (self.spark.table(tbl)
                       .join(broadcast(coddocs_df), on="coddoc", how="inner"))
            df_filt.persist(_SL)
            df_filt.count()
            df_filt.createOrReplaceTempView(tbl)
            logger.info("  %s: re-filtered to turmas_base scope.", tbl)
        for tbl in ["movimentofisico", "logprocesso", "movsobrestamento",
                    "processoassunto", "localvirtualdocumento"]:
            df_filt = (self.spark.table(tbl)
                       .join(broadcast(coddocs_df), on="coddoc", how="inner"))
            df_filt.createOrReplaceTempView(tbl)
        return qt

    # ── Stage 2: 15-step staged SQL ───────────────────────────────────────────

    def build_staged(self, v: str) -> DataFrame:
        """
        Run 15 staged materializations (mirrors turmas_staged_builder.py).
        Preconditions: turmas_base, all TempViews registered.
        Returns final un-persisted DataFrame (caller writes to Iceberg).
        """
        mat = lambda name, sql: self._mat(name, sql, v)

        # ── 0: complemento lookup ─────────────────────────────────────────────
        logger.info("step 0: complemento_stg")
        mat("complemento_stg", """
            SELECT codcompl,
                   CASE WHEN codcompl IN (9999, 59998) THEN NULL ELSE descr END AS descr,
                   descrmnem, indativo
            FROM complemento
        """)

        # ── 1: most recent movimentorelator per coddoc ────────────────────────
        logger.info("step 1: mov_relator_stg")
        mat("mov_relator_stg", """
            SELECT coddoc, codmagrel, codorgjulg
            FROM (
                SELECT mr.coddoc, mr.codmagrel, mr.codorgjulg,
                       ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov DESC) AS rn
                FROM movimentorelator mr
                WHERE mr.dthrmov <= '{v}'
            ) t WHERE rn = 1
        """)

        # ── 2: relatorio_base (magistrado / orgão names) ──────────────────────
        logger.info("step 2: relatorio_base_stg")
        mat("relatorio_base_stg", """
            SELECT '{v}' AS dth_referencia,
                   tb.coddoc,
                   tb.dataultdis AS dth_ultima_distribuicao,
                   pm.nome       AS ultimo_relator,
                   po.nome       AS ultimo_orgao_julgador,
                   mr.codmagrel  AS codultrel,
                   mr.codorgjulg AS codorgjulg
            FROM turmas_base tb
            LEFT JOIN mov_relator_stg mr ON tb.coddoc = mr.coddoc
            LEFT JOIN magistrado mag     ON mr.codmagrel  = mag.codmag
            LEFT JOIN pessoa pm          ON mag.codpess   = pm.codpess
            LEFT JOIN orgaojulgador oj   ON mr.codorgjulg = oj.codorgjulg
            LEFT JOIN pessoa po          ON oj.codpess    = po.codpess
        """)

        # ── 3: indicadores_processo ───────────────────────────────────────────
        logger.info("step 3: indicadores_stg")
        mat("indicadores_stg", """
            SELECT * FROM (
                SELECT p.coddoc,
                       p.indeletr        AS ind_eletronico,
                       p.codtipproc      AS cod_tipo_processo,
                       p.codcompt        AS cod_competencia,
                       p.indurgente      AS urgente,
                       p.indpedlimin     AS pedido_liminar,
                       p.inddefenpub     AS defensoria_publica,
                       p.indjustgrati    AS justica_gratuita,
                       p.indmenor        AS crianca_adolescente,
                       p.indmp           AS funciona_mp,
                       p.indsegrjust     AS segredo_justica,
                       p.indprioidoso    AS idosos,
                       p.indprioidoso80  AS idosos_80_mais,
                       p.inddoencagrave  AS doenca_grave,
                       p.indpriodefic    AS prioridade_deficiente,
                       p.indreupresopje  AS reu_preso,
                       p.indsitrua       AS situacao_rua,
                       tp.descr          AS classe,
                       p.numsequencial, p.dv, p.numano,
                       p.codjustica, p.codregi, p.codlocregi,
                       CASE WHEN p.indsupersigil = 'S' THEN 5
                            WHEN p.indsigil      = 'S' THEN 4
                            WHEN p.indsegrjust   = 'S' THEN 1
                            ELSE 0 END AS nivel_sigilo,
                       ROW_NUMBER() OVER (PARTITION BY p.coddoc ORDER BY p.dthralt DESC) AS rn
                FROM logprocesso p
                LEFT JOIN tipoprocesso tp ON p.codtipproc = tp.codtipproc
                WHERE p.tipoper != 'E'
                  AND p.dthralt <= '{v}'
            ) sub WHERE rn = 1
        """)

        # ── 4: ultimo_movimento ───────────────────────────────────────────────
        logger.info("step 4: ult_mov_stg")
        mat("ult_mov_stg", """
            SELECT coddoc,
                   dthrmov     AS dth_ultimo_movimento,
                   codfaseext  AS cod_ultimo_movimento,
                   fase_descr  AS ultimo_movimento,
                   c1_descr    AS comp1_ultimo_movimento,
                   c2_descr    AS comp2_ultimo_movimento,
                   c3_descr    AS comp3_ultimo_movimento,
                   usuincl     AS usuario_inclusao
            FROM (
                SELECT m.coddoc, m.dthrmov, fe.codfaseext,
                       f.descr  AS fase_descr,
                       c1.descr AS c1_descr,
                       c2.descr AS c2_descr,
                       c3.descr AS c3_descr,
                       m.usuincl,
                       ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
                FROM movimento m
                LEFT JOIN fase        f  ON m.codfase   = f.codfase
                LEFT JOIN faseexterna fe ON f.codfase   = fe.codfase
                LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
                LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
                LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
            ) t WHERE rn = 1
        """)

        # ── 5: ultimo_movimento_cnj (codfase != 67) ───────────────────────────
        logger.info("step 5: ult_mov_cnj_stg")
        mat("ult_mov_cnj_stg", """
            SELECT coddoc,
                   dthrmov    AS dth_ultimo_mov_cnj,
                   codfaseext AS cod_ultimo_mov_cnj,
                   fase_descr AS ultimo_mov_cnj,
                   codcompl1  AS comp1_cnj, c1_descr AS comp1_ultimo_mov_cnj,
                   codcompl2  AS comp2_cnj, c2_descr AS comp2_ultimo_mov_cnj,
                   codcompl3  AS comp3_cnj, c3_descr AS comp3_ultimo_mov_cnj,
                   usuincl    AS usuario_inclusao_cnj
            FROM (
                SELECT m.coddoc, m.dthrmov, fe.codfaseext,
                       f.descr  AS fase_descr,
                       m.codcompl1, c1.descr AS c1_descr,
                       m.codcompl2, c2.descr AS c2_descr,
                       m.codcompl3, c3.descr AS c3_descr,
                       m.usuincl,
                       ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
                FROM movimento m
                LEFT JOIN fase        f  ON m.codfase   = f.codfase
                LEFT JOIN faseexterna fe ON f.codfase   = fe.codfase
                LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
                LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
                LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
                WHERE m.codfase <> 67
            ) t WHERE rn = 1
        """)

        # ── 6: ultimo_julgamento (colegiado fase 193 OR monocrático fase 24) ──
        logger.info("step 6: ult_julg_stg (combined 193+24)")
        mat("ult_julg_stg", """
            SELECT coddoc,
                   dthrmov   AS dth_ultimo_julgamento,
                   fase_descr AS ultimo_julgamento,
                   c1_descr  AS comp1_ultimo_julgamento,
                   c2_descr  AS comp2_ultimo_julgamento,
                   c3_descr  AS comp3_ultimo_julgamento
            FROM (
                SELECT m.coddoc, m.dthrmov,
                       f.descr  AS fase_descr,
                       c1.descr AS c1_descr,
                       c2.descr AS c2_descr,
                       c3.descr AS c3_descr,
                       ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) rn
                FROM movimento m
                LEFT JOIN fase f ON m.codfase = f.codfase
                LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
                LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
                LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
                WHERE m.codfase IN (193, 24)
            ) t WHERE rn = 1
        """)

        logger.info("step 6b: ult_julg_monocratico_stg (fase 24)")
        mat("ult_julg_monocratico_stg", """
            SELECT coddoc, dthrmov AS dth_julgamento_monocratico
            FROM (
                SELECT coddoc, dthrmov,
                       ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) rn
                FROM movimento WHERE codfase = 24
            ) t WHERE rn = 1
        """)

        logger.info("step 6c: ult_julg_colegiado_stg (fase 193)")
        mat("ult_julg_colegiado_stg", """
            SELECT coddoc, dthrmov AS dth_julgamento_colegiado
            FROM (
                SELECT coddoc, dthrmov,
                       ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) rn
                FROM movimento WHERE codfase = 193
            ) t WHERE rn = 1
        """)

        # ── 7: local_virtual ──────────────────────────────────────────────────
        logger.info("step 7: local_virt_stg")
        mat("local_virt_stg", """
            SELECT t.coddoc,
                   t.dthrentr AS dth_entrada_local_virtual,
                   l.descr    AS local_virtual
            FROM (
                SELECT flv.coddoc, flv.dthrentr, flv.codlocalvirt, flv.codlocal,
                       ROW_NUMBER() OVER (PARTITION BY flv.coddoc ORDER BY flv.dthrentr DESC) AS rn
                FROM localvirtualdocumento flv
                WHERE flv.dthrentr <= '{v}'
            ) t
            LEFT JOIN localvirtual l ON t.codlocalvirt = l.codlocalvirt
                                    AND t.codlocal     = l.codlocal
            WHERE t.rn = 1
        """)

        # ── 8: assuntos ───────────────────────────────────────────────────────
        logger.info("step 8: assuntos_stg")
        mat("assuntos_stg", """
            SELECT pa.coddoc,
                   NULLIF(concat_ws(';', collect_list(CAST(pa.codass AS STRING))), '') AS cod_assunto,
                   NULLIF(concat_ws(';', collect_list(ass.descr)), '')                 AS assunto
            FROM processoassunto pa
            LEFT JOIN assunto ass ON pa.codass = ass.codass
            GROUP BY pa.coddoc
        """)

        # ── 9: vara_origem ────────────────────────────────────────────────────
        logger.info("step 9: vara_origem_stg")
        mat("vara_origem_stg", """
            SELECT CAST(CAST(a.coddoc AS int) AS string) AS coddoc,
                   CAST(aa.codvara AS int)               AS cod_vara,
                   p.nome                                AS vara
            FROM autoassociado aa
            LEFT JOIN vara v ON CAST(aa.codvara    AS int) = CAST(v.codvara    AS int)
                            AND CAST(aa.codjustorig AS int) = CAST(v.codjustorig AS int)
            LEFT JOIN pessoa p ON CAST(v.codpess AS int) = CAST(p.codpess AS int)
            LEFT JOIN turmas_base a ON CAST(aa.coddoc AS int) = CAST(a.coddoc AS int)
            WHERE aa.numseq = 1
        """)

        # ── 10: primeiro_relator ──────────────────────────────────────────────
        logger.info("step 10: prim_relator_stg")
        mat("prim_relator_stg", """
            SELECT coddoc, dthrmov AS dth_distribuicao, relator, orgao_julgador
            FROM (
                SELECT mr.coddoc, mr.dthrmov,
                       pm.nome AS relator,
                       po.nome AS orgao_julgador,
                       ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov ASC) rn
                FROM movimentorelator mr
                LEFT JOIN magistrado  mag ON mr.codmagrel  = mag.codmag
                LEFT JOIN pessoa      pm  ON mag.codpess   = pm.codpess
                LEFT JOIN orgaojulgador oj ON mr.codorgjulg = oj.codorgjulg
                LEFT JOIN pessoa      po  ON oj.codpess    = po.codpess
            ) t WHERE rn = 1
        """)

        # ── 11: local_fisico ──────────────────────────────────────────────────
        logger.info("step 11: local_fisico_stg")
        mat("local_fisico_stg", """
            SELECT coddoc, dthrmov AS dth_entrada_local_fisico, codlocal, local_fisico
            FROM (
                SELECT mf.coddoc, mf.dthrmov, mf.codlocal, lf.descr AS local_fisico,
                       ROW_NUMBER() OVER (PARTITION BY mf.coddoc ORDER BY mf.dthrmov DESC) rn
                FROM movimentofisico mf
                LEFT JOIN localfisico lf ON mf.codlocal = lf.codlocal
            ) t WHERE rn = 1
        """)

        # ── 11b: conclusao (last local fisico = gabinete de magistrado) ───────
        # Excludes locais 3142/3144/3148 (presidências, not relator's office)
        logger.info("step 11b: conclusao_stg")
        mat("conclusao_stg", """
            SELECT lfs.coddoc,
                   lfs.dth_entrada_local_fisico AS dth_conclusao
            FROM local_fisico_stg lfs
            JOIN localfisico lf ON lfs.codlocal = lf.codlocal
            WHERE lf.codmag IS NOT NULL
              AND lfs.codlocal NOT IN (3142, 3144, 3148)
        """)

        # ── 12: partes ────────────────────────────────────────────────────────
        logger.info("step 12: parte_stg")
        mat("parte_stg", """
            SELECT r.coddoc,
                   array_join(
                       filter(
                           array(
                               COALESCE(array_join(array_sort(collect_set(
                                   CASE WHEN CAST(tp.codtippolo AS INT) = 1
                                        THEN trim(cj.nome) END
                               )), '; '), ''),
                               COALESCE(array_join(array_sort(collect_set(
                                   CASE WHEN CAST(tp.codtippolo AS INT) = 2
                                        THEN trim(cj.nome) END
                               )), '; '), '')
                           ),
                           x -> x <> ''
                       ),
                       ' x '
                   ) AS partes
            FROM turmas_base r
            JOIN parte        p  ON r.coddoc     = p.coddoc
            JOIN pessoa       cj ON cj.codpess   = p.codpess
            JOIN parteprocesso pp ON pp.codparte = p.codparte
            JOIN tipoparte    tp ON tp.codtipparte = pp.codtipparte
            GROUP BY r.coddoc
        """)

        # ── 13a–13e: suspensao / repercussao / sobrestamento ─────────────────
        logger.info("step 13a: suspensao_stg")
        mat("suspensao_stg", """
            SELECT m.coddoc, m.dthrmov, m.codfase, m.codcompl1, m.codcompl2, m.codcompl3
            FROM movimento m
            WHERE m.dthrmov <= '{v}'
              AND (
                  (m.codfase IN (21, 30, 11009) AND m.codcompl1 = 11025)
               OR (m.codfase IN (3, 20)          AND m.codcompl1 = 25)
               OR (m.codfase IN (3, 20, 29)      AND m.codcompl1 = 1013
                   AND COALESCE(m.codcompl2, 0) = 1015)
               OR (m.codfase = 861               AND m.codcompl1 IN (690, 50068))
               OR (m.codfase = 861               AND m.codcompl1 IN (359,360,361,362,363,445,50069)
                   AND COALESCE(m.codcompl2, 0) IN (413, 414, 421))
               OR (m.codfase = 50002             AND m.codcompl1 IN (306, 12065))
              )
        """)

        logger.info("step 13b: reativacao_stg")
        mat("reativacao_stg", """
            SELECT m.coddoc, m.dthrmov
            FROM movimento m
            WHERE m.dthrmov <= '{v}'
              AND (
                  (m.codfase = 50002 AND m.codcompl1 = 12066)
               OR  m.codfase IN (893, 24, 193)
              )
        """)

        logger.info("step 13c: repercussao_stg")
        mat("repercussao_stg", """
            SELECT x.coddoc, x.dthrmov, x.codfase, x.codcompl1, x.codcompl2, x.codcompl3,
                   x.dthrterminosobre
            FROM (
                SELECT m.coddoc, m.dthrmov, m.codfase, m.codcompl1, m.codcompl2, m.codcompl3,
                       CASE WHEN ms.dthrterminosobre >= '{v}' THEN NULL
                            ELSE ms.dthrterminosobre END AS dthrterminosobre
                FROM movimento m
                JOIN movsobrestamento ms ON ms.coddoc  = m.coddoc
                                        AND ms.dthrmov = m.dthrmov
                WHERE m.dthrmov <= '{v}'
                  AND m.codfase  = 50040
            ) x
            WHERE dthrterminosobre IS NULL
        """)

        logger.info("step 13d: susp_rep_agg_stg")
        mat("susp_rep_agg_stg", """
            SELECT x.coddoc,
                   MIN(x.dthrmov)                                  AS datasusp,
                   MIN(CASE WHEN x.src = 'R' THEN x.dthrmov END)  AS datarepercussao
            FROM (
                SELECT s.coddoc, s.dthrmov, 'S' AS src
                FROM suspensao_stg s
                LEFT JOIN reativacao_stg r ON r.coddoc  = s.coddoc
                                          AND r.dthrmov > s.dthrmov
                WHERE r.coddoc IS NULL
                UNION ALL
                SELECT r.coddoc, r.dthrmov, 'R' AS src
                FROM repercussao_stg r
            ) x
            GROUP BY x.coddoc
        """)

        logger.info("step 13e: susp_rep_stg")
        mat("susp_rep_stg", """
            SELECT acer.coddoc,
                   a.datasusp                                          AS dth_suspensao,
                   f_susp.descr                                        AS movimento_suspensao,
                   c1_susp.descr                                       AS comp1_ultima_suspensao,
                   c2_susp.descr                                       AS comp2_ultima_suspensao,
                   c3_susp.descr                                       AS comp3_ultima_suspensao,
                   CASE WHEN a.datasusp IS NOT NULL THEN 'S' ELSE 'N' END AS ind_suspenso,
                   a.datarepercussao                                   AS dth_repercussao,
                   a.datarepercussao                                   AS dth_sobrestamento,
                   r.dthrterminosobre,
                   f_rep.descr                                         AS movimento_sobrestamento,
                   c1_rep.descr                                        AS comp1_ultimo_sobrestamento,
                   c2_rep.descr                                        AS comp2_ultimo_sobrestamento,
                   c3_rep.descr                                        AS comp3_ultimo_sobrestamento,
                   CASE WHEN a.datarepercussao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_sobrestado
            FROM turmas_base acer
            LEFT JOIN susp_rep_agg_stg  a       ON acer.coddoc  = a.coddoc
            LEFT JOIN suspensao_stg     s       ON s.coddoc     = a.coddoc
                                               AND s.dthrmov    = a.datasusp
            LEFT JOIN fase              f_susp  ON s.codfase    = f_susp.codfase
            LEFT JOIN complemento_stg   c1_susp ON s.codcompl1  = c1_susp.codcompl
            LEFT JOIN complemento_stg   c2_susp ON s.codcompl2  = c2_susp.codcompl
            LEFT JOIN complemento_stg   c3_susp ON s.codcompl3  = c3_susp.codcompl
            LEFT JOIN repercussao_stg   r       ON r.coddoc     = a.coddoc
                                               AND r.dthrmov    = a.datarepercussao
            LEFT JOIN fase              f_rep   ON r.codfase    = f_rep.codfase
            LEFT JOIN complemento_stg   c1_rep  ON r.codcompl1  = c1_rep.codcompl
            LEFT JOIN complemento_stg   c2_rep  ON r.codcompl2  = c2_rep.codcompl
            LEFT JOIN complemento_stg   c3_rep  ON r.codcompl3  = c3_rep.codcompl
        """)

        # ── 14: decisao_final ─────────────────────────────────────────────────
        logger.info("step 14: decisao_stg")
        mat("decisao_stg", """
            SELECT coddoc, dth_ultima_decisao, ultima_decisao,
                   comp1_ultima_decisao, comp2_ultima_decisao, comp3_ultima_decisao,
                   CASE WHEN dth_ultima_decisao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_decisao
            FROM (
                SELECT m.coddoc,
                       m.dthrmov  AS dth_ultima_decisao,
                       f.descr    AS ultima_decisao,
                       c1.descr   AS comp1_ultima_decisao,
                       c2.descr   AS comp2_ultima_decisao,
                       c3.descr   AS comp3_ultima_decisao,
                       ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
                FROM movimento m
                LEFT JOIN fase f ON m.codfase = f.codfase
                LEFT JOIN complemento_stg c1 ON m.codcompl1 = c1.codcompl
                LEFT JOIN complemento_stg c2 ON m.codcompl2 = c2.codcompl
                LEFT JOIN complemento_stg c3 ON m.codcompl3 = c3.codcompl
                WHERE m.codfase IN (3, 20)
                  AND m.dthrmov <= '{v}'
            ) u WHERE rn = 1
        """)

        # ── 14a–14g: exclusion flag sources ───────────────────────────────────
        logger.info("step 14a: excl_fase_simples_stg")
        mat("excl_fase_simples_stg", """
            SELECT DISTINCT m.coddoc
            FROM movimento m JOIN turmas_base tb ON m.coddoc = tb.coddoc
            WHERE m.dthrmov <= '{v}' AND m.dthrmov >= tb.dataultdis
              AND m.codfase IN (861, 870, 22, 50011, 50014)
        """)

        logger.info("step 14b: excl_findo_stg")
        mat("excl_findo_stg", """
            SELECT DISTINCT m.coddoc
            FROM movimento m JOIN turmas_base tb ON m.coddoc = tb.coddoc
            WHERE m.dthrmov <= '{v}' AND m.dthrmov >= tb.dataultdis
              AND m.codfase = 50002
              AND (m.codcompl1 IN (50117, 50118, 60435)
                OR COALESCE(m.codcompl2, 0) = 50118)
        """)

        logger.info("step 14c: excl_expedicao_stg")
        mat("excl_expedicao_stg", """
            SELECT DISTINCT m.coddoc
            FROM movimento m JOIN turmas_base tb ON m.coddoc = tb.coddoc
            WHERE m.dthrmov <= '{v}' AND m.dthrmov >= tb.dataultdis
              AND m.codfase = 60 AND m.codcompl1 = 50007
              AND COALESCE(m.codcompl2, 0) = 50119
        """)

        logger.info("step 14d: excl_remessa_stg")
        mat("excl_remessa_stg", """
            SELECT DISTINCT m.coddoc
            FROM movimento m JOIN turmas_base tb ON m.coddoc = tb.coddoc
            WHERE m.dthrmov <= '{v}' AND m.dthrmov >= tb.dataultdis
              AND m.codfase IN (50123, 123)
              AND (m.codcompl1 IN (50104, 50648, 50105, 50649, 50723)
                OR COALESCE(m.codcompl2, 0) IN (50052, 50079, 50080))
        """)

        logger.info("step 14e: excl_remessa_esp_stg")
        mat("excl_remessa_esp_stg", """
            SELECT DISTINCT m.coddoc
            FROM movimento m JOIN turmas_base tb ON m.coddoc = tb.coddoc
            WHERE m.dthrmov <= '{v}' AND m.dthrmov >= tb.dataultdis
              AND m.codfase = 123 AND m.codcompl1 = 66653
              AND COALESCE(m.codcompl2, 0) = 66652
        """)

        logger.info("step 14f: excl_local_saida_stg")
        mat("excl_local_saida_stg", """
            SELECT DISTINCT mf.coddoc
            FROM movimentofisico mf JOIN turmas_base tb ON mf.coddoc = tb.coddoc
            WHERE mf.dthrmov <= '{v}' AND mf.dthrmov >= tb.dataultdis
              AND mf.codlocal IN (
                  503,  515,  519,  520,  521,  549,
                  583,  584,
                  419,  420,  421,  422,  423,  424,  425,  426,  427,  428,  429,  430,
                  1713, 1714, 1715, 1771,
                  2085, 2594, 2593,
                  3146, 3147, 3148, 3149, 3150, 3151,
                  3205, 3206,
                  3582, 3583, 3584, 3585,
                  3875, 5765
              )
        """)

        logger.info("step 14g: excl_suspensao_stg")
        mat("excl_suspensao_stg", """
            SELECT DISTINCT s.coddoc
            FROM (
                SELECT t.coddoc, t.dthrmov
                FROM (
                    SELECT m.coddoc, m.dthrmov,
                           ROW_NUMBER() OVER (PARTITION BY m.coddoc
                                             ORDER BY m.dthrmov DESC) AS rn
                    FROM movimento m
                    JOIN turmas_base tb ON m.coddoc = tb.coddoc
                    WHERE m.dthrmov <= '{v}' AND m.dthrmov >= tb.dataultdis
                      AND m.codfase   IN (3, 11009)
                      AND m.codcompl1 IN (25, 11025)
                ) t WHERE t.rn = 1
            ) s
            LEFT JOIN (
                SELECT m.coddoc, m.dthrmov
                FROM movimento m
                WHERE m.dthrmov <= '{v}' AND m.codfase = 849
            ) r ON r.coddoc = s.coddoc AND r.dthrmov > s.dthrmov
            WHERE r.coddoc IS NULL
        """)


        # ── 15: final assembly ────────────────────────────────────────────────
        logger.info("step 15: assembling relatorio_final")
        df_final = self.spark.sql("""
            SELECT
                CAST(rb.dth_referencia AS DATE)  AS dth_referencia,
                rb.coddoc,
                rb.dth_ultima_distribuicao,
                rb.ultimo_relator,
                rb.ultimo_orgao_julgador,
                ip.ind_eletronico,
                ip.cod_tipo_processo,
                ip.cod_competencia,
                ip.urgente,
                ip.pedido_liminar,
                ip.defensoria_publica,
                ip.justica_gratuita,
                ip.crianca_adolescente,
                ip.funciona_mp,
                ip.segredo_justica,
                ip.idosos,
                ip.idosos_80_mais,
                ip.doenca_grave,
                ip.prioridade_deficiente,
                ip.reu_preso,
                ip.situacao_rua,
                ip.classe,
                ip.numsequencial,
                ip.dv,
                ip.numano,
                ip.codjustica,
                ip.codregi,
                ip.codlocregi,
                um.dth_ultimo_movimento,
                um.cod_ultimo_movimento,
                um.ultimo_movimento,
                um.comp1_ultimo_movimento,
                um.comp2_ultimo_movimento,
                um.comp3_ultimo_movimento,
                um.usuario_inclusao,
                uj.dth_ultimo_julgamento,
                uj.ultimo_julgamento,
                uj.comp1_ultimo_julgamento,
                uj.comp2_ultimo_julgamento,
                uj.comp3_ultimo_julgamento,
                CASE WHEN uj.dth_ultimo_julgamento IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgado,
                ujm.dth_julgamento_monocratico,
                CASE WHEN ujm.dth_julgamento_monocratico IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento_monocratico,
                ujc.dth_julgamento_colegiado,
                CASE WHEN ujc.dth_julgamento_colegiado IS NOT NULL THEN 'S' ELSE 'N' END AS ind_julgamento_colegiado,
                lv.dth_entrada_local_virtual,
                lv.local_virtual,
                asu.cod_assunto,
                asu.assunto,
                vo.cod_vara,
                vo.vara,
                usv.dth_suspensao,
                usv.movimento_suspensao,
                usv.comp1_ultima_suspensao,
                usv.comp2_ultima_suspensao,
                usv.dth_sobrestamento,
                usv.dthrterminosobre,
                usv.movimento_sobrestamento,
                usv.comp1_ultimo_sobrestamento,
                usv.comp2_ultimo_sobrestamento,
                usv.ind_sobrestado,
                pr.dth_distribuicao,
                pr.relator,
                pr.orgao_julgador,
                lf.dth_entrada_local_fisico,
                lf.local_fisico,
                mc.dth_conclusao,
                CASE WHEN mc.dth_conclusao IS NOT NULL THEN 'S' ELSE 'N' END AS ind_conclusao,
                CASE WHEN ip.nivel_sigilo = 5 THEN 'RESTRITO' ELSE
                    lpad(CAST(ip.numsequencial AS STRING), 7, '0') || '-' ||
                    lpad(CAST(ip.dv AS STRING), 2, '0')           || '.' ||
                    CAST(ip.numano AS STRING)                      || '.' ||
                    CAST(ip.codjustica AS STRING)                  || '.' ||
                    lpad(CAST(ip.codregi AS STRING), 2, '0')       || '.' ||
                    lpad(CAST(ip.codlocregi AS STRING), 4, '0')
                END AS num_processo,
                rb.codultrel    AS cod_ultimo_relator,
                rb.codorgjulg   AS cod_ultimo_orgao_julgador,
                usv.ind_suspenso,
                CASE WHEN ip.nivel_sigilo = 5 THEN 'RESTRITO' ELSE parte.partes END AS partes,
                usv.comp3_ultima_suspensao,
                usv.dth_repercussao,
                usv.comp3_ultimo_sobrestamento,
                ud.dth_ultima_decisao,
                ud.ultima_decisao,
                ud.comp1_ultima_decisao,
                ud.comp2_ultima_decisao,
                ud.comp3_ultima_decisao,
                COALESCE(ud.ind_decisao, 'N') AS ind_decisao,
                ip.nivel_sigilo,
                lf.codlocal     AS cod_ult_local,
                um1.dth_ultimo_mov_cnj      AS dth_ultimo_movimento_d01,
                um1.cod_ultimo_mov_cnj      AS cod_ultimo_movimento_d01,
                um1.ultimo_mov_cnj          AS ultimo_movimento_d01,
                um1.comp1_cnj               AS cod_comp1_d01,
                um1.comp1_ultimo_mov_cnj    AS comp1_ultimo_movimento_d01,
                um1.comp2_cnj               AS cod_comp2_d01,
                um1.comp2_ultimo_mov_cnj    AS comp2_ultimo_movimento_d01,
                um1.comp3_cnj               AS cod_comp3_d01,
                um1.comp3_ultimo_mov_cnj    AS comp3_ultimo_movimento_d01,
                -- exclusion flags (grupos RIGER A..J)
                CASE WHEN ef1.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_fase_simples,
                CASE WHEN ef2.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_findo,
                CASE WHEN ef3.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_expedicao,
                CASE WHEN ef4.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_remessa,
                CASE WHEN ef5.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_remessa_esp,
                CASE WHEN ef6.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_local_saida,
                CASE WHEN ef7.coddoc IS NOT NULL THEN 'S' ELSE 'N' END AS ind_excl_suspensao,
                -- ind_acervo_riger: no group A-H (CNJ 6.5)
                CASE WHEN ef1.coddoc IS NULL
                      AND ef2.coddoc IS NULL
                      AND ef3.coddoc IS NULL
                      AND ef4.coddoc IS NULL
                      AND ef5.coddoc IS NULL
                      AND ef6.coddoc IS NULL
                      AND ef7.coddoc IS NULL
                     THEN 'S' ELSE 'N' END AS ind_acervo_riger,
                -- ind_acervo_total_cnj: no group A-G; H (suspensão) INCLUDED (CNJ 6.3)
                CASE WHEN ef1.coddoc IS NULL
                      AND ef2.coddoc IS NULL
                      AND ef3.coddoc IS NULL
                      AND ef4.coddoc IS NULL
                      AND ef5.coddoc IS NULL
                      AND ef6.coddoc IS NULL
                     THEN 'S' ELSE 'N' END AS ind_acervo_total_cnj
            FROM relatorio_base_stg rb
            LEFT JOIN indicadores_stg          ip    ON rb.coddoc = ip.coddoc
            LEFT JOIN ult_mov_stg              um    ON rb.coddoc = um.coddoc
            LEFT JOIN ult_mov_cnj_stg          um1   ON rb.coddoc = um1.coddoc
            LEFT JOIN ult_julg_stg             uj    ON rb.coddoc = uj.coddoc
            LEFT JOIN ult_julg_monocratico_stg ujm   ON rb.coddoc = ujm.coddoc
            LEFT JOIN ult_julg_colegiado_stg   ujc   ON rb.coddoc = ujc.coddoc
            LEFT JOIN local_virt_stg           lv    ON rb.coddoc = lv.coddoc
            LEFT JOIN assuntos_stg             asu   ON rb.coddoc = asu.coddoc
            LEFT JOIN vara_origem_stg          vo    ON rb.coddoc = vo.coddoc
            LEFT JOIN susp_rep_stg             usv   ON rb.coddoc = usv.coddoc
            LEFT JOIN prim_relator_stg         pr    ON rb.coddoc = pr.coddoc
            LEFT JOIN local_fisico_stg         lf    ON rb.coddoc = lf.coddoc
            LEFT JOIN conclusao_stg            mc    ON rb.coddoc = mc.coddoc
            LEFT JOIN parte_stg                parte ON rb.coddoc = parte.coddoc
            LEFT JOIN decisao_stg              ud    ON rb.coddoc = ud.coddoc
            LEFT JOIN excl_fase_simples_stg    ef1   ON rb.coddoc = ef1.coddoc
            LEFT JOIN excl_findo_stg           ef2   ON rb.coddoc = ef2.coddoc
            LEFT JOIN excl_expedicao_stg       ef3   ON rb.coddoc = ef3.coddoc
            LEFT JOIN excl_remessa_stg         ef4   ON rb.coddoc = ef4.coddoc
            LEFT JOIN excl_remessa_esp_stg     ef5   ON rb.coddoc = ef5.coddoc
            LEFT JOIN excl_local_saida_stg     ef6   ON rb.coddoc = ef6.coddoc
            LEFT JOIN excl_suspensao_stg       ef7   ON rb.coddoc = ef7.coddoc
        """)
        return df_final

    # ── Orchestration ─────────────────────────────────────────────────────────

    def calculate(self, dateref_str: str) -> DataFrame:
        logger.info("=" * 70)
        logger.info("EJUD Turmas Acervo | dateref=%s", dateref_str)
        logger.info("=" * 70)
        self.load_bronze_tables(dateref_str)
        self.build_turmas_base(dateref_str)
        return self.build_staged(dateref_str)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="EJUD Turmas Acervo — daily snapshot to Iceberg (dw_2inst_ouro)"
    )
    p.add_argument("--dateref", required=True,
                   help="Reference date YYYY-MM-DD (cutoff = 23:59:59 of that day)")
    p.add_argument("--catalog",       default=_CATALOG)
    p.add_argument("--db-bronze",     default="ejud",          dest="db_bronze")
    p.add_argument("--db-ouro",       default=_TARGET_SCHEMA,  dest="db_ouro")
    p.add_argument("--target-table",  default=_TARGET_TABLE,   dest="target_table")
    p.add_argument("--executor-memory",          default="18g",  dest="executor_memory")
    p.add_argument("--executor-memory-overhead", default="4g",   dest="executor_memory_overhead")
    p.add_argument("--shuffle-partitions",       default=64, type=int, dest="shuffle_partitions")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Build full dateref string (23:59:59 cutoff, same as linux version)
    dt = datetime.strptime(args.dateref, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    v_dateref_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    logger.info("=" * 70)
    logger.info("EJUD Turmas Acervo | dateref=%s | cutoff=%s", args.dateref, v_dateref_str)
    logger.info("target: %s.%s.%s", args.catalog, args.db_ouro, args.target_table)
    logger.info("=" * 70)

    spark = _create_spark_session(
        app_name=f"EJUD_Turmas_Acervo_{args.dateref}",
        executor_memory=args.executor_memory,
        executor_memory_overhead=args.executor_memory_overhead,
        shuffle_partitions=args.shuffle_partitions,
    )
    logger.info("Spark %s ready", spark.version)

    try:
        calc = TurmasAcervoCalculator(
            spark,
            catalog=args.catalog,
            db_bronze=args.db_bronze,
            db_ouro=args.db_ouro,
        )
        df = calc.calculate(v_dateref_str)
        n = df.count()
        logger.info("Final result: %d rows", n)
        _save_to_iceberg(df, schema=args.db_ouro, table=args.target_table)
        logger.info("Done — %d rows written | dateref=%s", n, args.dateref)
    except Exception as exc:
        logger.error("Job failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
