#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_metacnj_meta_aux_emr.py — All-systems aux table builder (ejud + eproc2g)

Reads dw_2inst_ouro.tb_ft_meta_analitico (written by ejud_metacnj_m??_emr.py AND
eproc_metacnj_m01_emr.py), deduplicates to one row per process for ALL systems,
and writes dw_2inst_ouro.tb_ft_meta_aux in a single DELETE+APPEND.

Handles:
  cod_sistema = 1  — EJUD       (bronze DB: ejud)
  cod_sistema = 2  — eproc2g    (bronze DB: eproc2g)

This replicates the role of dw2inst.fato_auxiliar in the old Linux pipeline,
but fully cloud-native (Iceberg, Glue Catalog). No postgres, no local parquet.

Target: glue_catalog.dw_2inst_ouro.tb_ft_meta_aux
Schema matches production DDL:
    id_processo, dth_prim_distribuicao, dth_ult_distribuicao,
    cod_prim_oj, cod_ult_oj, cod_prim_magistrado, cod_ult_magistrado,
    dth_ult_evento, cod_ult_evento_cnj, ind_baixado, ind_julgado,
    id_sigilo, complemento, meta_ano

Deduplication logic (per id_processo within meta_ano):
    - dth_prim_distribuicao / dth_ult_distribuicao:
        earliest / latest dth_evento_indicador among DISTRIBUTION-type rows only
        (cod_indicador IN DIST_COD_INDICADORES — cnm/dism* variables).
        Using all rows caused judgment/suspension dates to pollute distribution dates.
    - cod_prim_oj / cod_prim_magistrado: from the earliest distribution row
    - cod_ult_oj / cod_ult_magistrado:   from the latest distribution row
    - dth_ult_evento:
        ejud    — dthrmov from ejud.movimentorelator (latest movement)
        eproc2g — dth_evento from eproc2g.processo_evento (latest event)
    - cod_ult_evento_cnj:
        ejud    — NULL (movimentorelator has no CNJ code column)
        eproc2g — cod_mni_movimento from eproc2g.evento_judicial
    - ind_julgado: 'S' if any julgadom* cod_indicador is present for the process
    - ind_baixado: NULL (baixm* not stored independently in tb_ft_meta_analitico)
    - id_sigilo: from first non-null distribution row
    - complemento: NULL

Run AFTER ALL indicator scripts for the target meta_ano
(ejud_metacnj_m??.py AND eproc_metacnj_m01_emr.py).

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts/ejud_metacnj_meta_aux_emr.py \\
      --year 2026 2>&1 | \\
      grep --line-buffered -E 'ejud_metacnj_meta_aux|ERROR|Exception'
"""

import argparse
import logging
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, regexp_replace, row_number

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_metacnj_meta_aux_emr")


# ── Constants ─────────────────────────────────────────────────────────────────
_CATALOG           = "glue_catalog"
_WAREHOUSE         = "s3://sgdai-data-lake"
_SOURCE_DB         = "dw_2inst_ouro"
_SOURCE_TABLE      = "tb_ft_meta_analitico"
_TARGET_DB         = "dw_2inst_ouro"
_TARGET_TABLE      = "tb_ft_meta_aux"
_TARGET_LOC        = f"{_WAREHOUSE}/ouro/dw_2inst_ouro/{_TARGET_TABLE}"
_DB_BRONZE_EJUD    = "ejud"      # bronze catalog DB for cod_sistema = 1
_DB_BRONZE_EPROC   = "eproc2g"  # bronze catalog DB for cod_sistema = 2

# cod_indicador values that represent a distribution event
# (dth_evento_indicador == date the process entered the stock/period).
# Used exclusively for dth_prim/ult_distribuicao and the associated OJ/magistrado.
# Source: COD_INDICADOR_MAP in each m??_emr.py script.
DIST_COD_INDICADORES = {
    1,   # cnm01
    6,   # dism02
    11,  # dism02_ant
    13,  # dism04
    20,  # dism06
    48,  # dism07
    27,  # dism08
    34,  # dism10
    41,  # dism11
}

# cod_indicador values that represent a first-judgment event.
JULGADO_COD_INDICADORES = {
    2,   # julgadom01
    7,   # julgadom02
    10,  # julgadom02_ant
    14,  # julgadom04
    21,  # julgadom06
    49,  # julgadom07
    28,  # julgadom08
    35,  # julgadom10
    42,  # julgadom11
}


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudMetaCNJ_MetaAux")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"{_WAREHOUSE}/warehouse")
        .config("spark.sql.defaultCatalog", _CATALOG)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


# ── Iceberg helpers ───────────────────────────────────────────────────────────
def _ensure_target_table(spark: SparkSession) -> None:
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            id_processo           STRING,
            dth_prim_distribuicao TIMESTAMP,
            dth_ult_distribuicao  TIMESTAMP,
            cod_prim_oj           BIGINT,
            cod_ult_oj            BIGINT,
            cod_prim_magistrado   BIGINT,
            cod_ult_magistrado    BIGINT,
            dth_ult_evento        TIMESTAMP,
            cod_ult_evento_cnj    STRING,
            ind_baixado           STRING,
            ind_julgado           STRING,
            id_sigilo             INT,
            complemento           STRING,
            meta_ano              INT
        )
        USING iceberg
        PARTITIONED BY (meta_ano)
        LOCATION '{_TARGET_LOC}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    logger.info("Ensured target table: %s", fqn)


def _save_to_iceberg(spark: SparkSession, df: DataFrame, meta_ano_int: int) -> None:
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    _ensure_target_table(spark)
    spark.sql(f"DELETE FROM {fqn} WHERE meta_ano = {meta_ano_int}")
    logger.info("Writing → %s", fqn)
    df.writeTo(fqn).append()
    logger.info("Write complete: %s", fqn)


# ── Builder ───────────────────────────────────────────────────────────────────
class MetaAuxBuilder:

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = _CATALOG,
        db_source: str = _SOURCE_DB,
        db_bronze: str = _DB_BRONZE_EJUD,
        db_bronze_eproc: str = _DB_BRONZE_EPROC,
    ):
        self.spark          = spark
        self.cat            = catalog
        self.db_source      = db_source
        self.db_bronze      = db_bronze
        self.db_bronze_eproc = db_bronze_eproc

    def _tbl(self, db: str, table: str) -> str:
        return f"{self.cat}.{db}.{table}"

    def _norm(self, c):
        return regexp_replace(c.cast("string"), r"\.0$", "")

    def build(self, meta_ano: int) -> DataFrame:
        source = (
            self.spark.table(self._tbl(self.db_source, _SOURCE_TABLE))
            .filter(col("meta_ano") == lit(meta_ano))
        )
        source.cache()
        logger.info("Source loaded and cached for meta_ano=%d", meta_ano)

        # ── Distribution rows only (for first/last dist dates + OJ/magistrado) ──
        # Using all rows caused judgment/suspension dth_evento_indicador values to
        # pollute dth_prim/ult_distribuicao — the root cause of the dedup problem.
        dist_rows = source.filter(col("cod_indicador").isin(list(DIST_COD_INDICADORES)))

        w_asc  = Window.partitionBy("id_processo").orderBy(col("dth_evento_indicador").asc())
        w_desc = Window.partitionBy("id_processo").orderBy(col("dth_evento_indicador").desc())

        first_dist = (
            dist_rows
            .withColumn("_rn", row_number().over(w_asc))
            .filter(col("_rn") == 1)
            .select(
                col("id_processo"),
                col("dth_evento_indicador").alias("dth_prim_distribuicao"),
                col("cod_ult_oj").cast("bigint").alias("cod_prim_oj"),
                col("cod_ult_magistrado").cast("bigint").alias("cod_prim_magistrado"),
                col("id_sigilo").cast("int"),
                col("cod_sistema"),  # propagated for per-system last-event routing
            )
        )

        last_dist = (
            dist_rows
            .withColumn("_rn", row_number().over(w_desc))
            .filter(col("_rn") == 1)
            .select(
                col("id_processo"),
                col("dth_evento_indicador").alias("dth_ult_distribuicao"),
                col("cod_ult_oj").cast("bigint"),
                col("cod_ult_magistrado").cast("bigint"),
            )
        )

        # ind_julgado: 'S' if any julgadom* row exists for this process
        # ind_baixado: NULL — baixm* sub-components are not stored independently
        #              in tb_ft_meta_analitico (they are folded into julgadom*)
        ind_julgado = (
            source
            .filter(col("cod_indicador").isin(list(JULGADO_COD_INDICADORES)))
            .select("id_processo")
            .distinct()
            .withColumn("ind_julgado", lit("S"))
        )

        # ── Base dedup ────────────────────────────────────────────────────────
        base = (
            first_dist
            .join(last_dist,   on="id_processo", how="left")
            .join(ind_julgado, on="id_processo", how="left")
            .fillna({"ind_julgado": "N"})
            .withColumn("ind_baixado", lit(None).cast("string"))
        )

        source.unpersist()

        # ── Last event: ejud — ejud.movimentorelator ──────────────────────────
        # movimentorelator has no CNJ movement code so cod_ult_evento_cnj = NULL
        ejud_ids = base.filter(col("cod_sistema") == 1).select("id_processo").distinct()
        w_ejud = Window.partitionBy("id_processo").orderBy(col("dth_ult_evento").desc())
        ejud_last_mov = (
            self.spark.table(self._tbl(self.db_bronze, "movimentorelator"))
            .join(ejud_ids.withColumnRenamed("id_processo", "coddoc"), on="coddoc", how="semi")
            .select(
                col("coddoc").alias("id_processo"),
                col("dthrmov").alias("dth_ult_evento"),
                lit(None).cast("string").alias("cod_ult_evento_cnj"),
            )
            .withColumn("_rn", row_number().over(w_ejud))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # ── Last event: eproc2g — processo_evento + evento_judicial ───────────
        # cod_mni_movimento from evento_judicial is the CNJ movement code.
        logger.info("Loading last events (ejud=movimentorelator, eproc=processo_evento)")
        eproc_ids = base.filter(col("cod_sistema") == 2).select("id_processo").distinct()
        w_eproc = Window.partitionBy("id_processo").orderBy(col("dth_ult_evento").desc())
        eproc_last_mov = (
            self.spark.table(self._tbl(self.db_bronze_eproc, "processo_evento"))
            .join(eproc_ids, on="id_processo", how="semi")
            .select("id_processo", "dth_evento", "id_evento_judicial")
            .join(
                self.spark.table(self._tbl(self.db_bronze_eproc, "evento_judicial"))
                .select(
                    col("id_evento_judicial"),
                    col("cod_mni_movimento").alias("cod_ult_evento_cnj"),
                ),
                on="id_evento_judicial",
                how="left",
            )
            .withColumnRenamed("dth_evento", "dth_ult_evento")
            .drop("id_evento_judicial")
            .withColumn("_rn", row_number().over(w_eproc))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # ── Union both systems' last events ───────────────────────────────────
        last_mov = ejud_last_mov.unionByName(eproc_last_mov, allowMissingColumns=True)

        # ── Final join and output ─────────────────────────────────────────────
        result = (
            base
            .join(last_mov, on="id_processo", how="left")
            .select(
                col("id_processo"),
                col("dth_prim_distribuicao"),
                col("dth_ult_distribuicao"),
                col("cod_prim_oj"),
                col("cod_ult_oj"),
                col("cod_prim_magistrado"),
                col("cod_ult_magistrado"),
                col("dth_ult_evento"),
                col("cod_ult_evento_cnj"),   # NULL for ejud, cod_mni_movimento for eproc
                col("ind_baixado"),
                col("ind_julgado"),
                col("id_sigilo"),
                lit(None).cast("string").alias("complemento"),
                lit(meta_ano).cast("int").alias("meta_ano"),
            )
            # cod_sistema was carried through base but is not in the target DDL
        )

        logger.info("meta_aux built for meta_ano=%d", meta_ano)
        return result

    def run(self, meta_ano: int) -> None:
        logger.info("=" * 70)
        logger.info("ejud_metacnj_meta_aux_emr  meta_ano=%d", meta_ano)
        logger.info("=" * 70)
        df = self.build(meta_ano)
        _save_to_iceberg(self.spark, df, meta_ano)
        logger.info("=" * 70)
        logger.info("ejud_metacnj_meta_aux_emr  DONE  meta_ano=%d", meta_ano)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Build dw_2inst_ouro.tb_ft_meta_aux from dw_2inst_ouro.tb_ft_meta_analitico"
    )
    p.add_argument("--year",  type=int, default=date.today().year,
                   help="Stock year / meta_ano (default: current year)")
    p.add_argument("--catalog",                  default=_CATALOG)
    p.add_argument("--db-source",                default=_SOURCE_DB)
    p.add_argument("--db-bronze",                default=_DB_BRONZE_EJUD)
    p.add_argument("--db-bronze-eproc",          default=_DB_BRONZE_EPROC)
    p.add_argument("--executor-memory",          default="12g")
    p.add_argument("--executor-memory-overhead", default="2g")
    p.add_argument("--shuffle-partitions", type=int, default=200)
    return p.parse_args(argv)


def main(argv=None):
    args  = _parse_args(argv)
    spark = _create_spark_session(
        args.executor_memory,
        args.executor_memory_overhead,
        args.shuffle_partitions,
    )
    builder = MetaAuxBuilder(
        spark=spark,
        catalog=args.catalog,
        db_source=args.db_source,
        db_bronze=args.db_bronze,
        db_bronze_eproc=args.db_bronze_eproc,
    )
    try:
        builder.run(meta_ano=args.year)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
