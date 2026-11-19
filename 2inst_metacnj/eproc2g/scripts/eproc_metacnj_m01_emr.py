#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eproc_metacnj_m01_emr.py — EPROC2G Meta 01 → Iceberg (ouro layer)

Single-script design: no external .py dependencies.
All data reads via spark.table() from the Glue catalog.

Source   : glue_catalog.dw_2inst_ouro.tb_tf_situacao_eproc
           s3://sgdai-data-lake/ouro/dw_2inst_ouro/tb_tf_situacao_eproc
           Partition: months(dth_inicio)
           Columns used: id_processo STRING, dth_inicio TIMESTAMP, situacao INT,
                         id_classe_judicial INT  ← already CNJ MNI code
                         id_assunto_principal INT ← internal eproc2g ID (resolved below)
                         id_orgao_juizo INT, id_orgao_colegiado INT, id_sigilo INT,
                         cod_mni_movimento STRING, num_processo STRING

Bronze   : glue_catalog.eproc2g.assunto_judicial
           glue_catalog.eproc2g.processo
           glue_catalog.eproc2g.competencia_judicial

Target   : glue_catalog.dw_2inst_ouro.tb_ft_meta_analitico
           s3://sgdai-data-lake/ouro/dw_2inst_ouro/tb_ft_meta_analitico
           Partition: (meta_ano INT)

Write strategy (CRITICAL):
    ejud m01 writes cod_sistema=1 rows with overwritePartitions() — replaces entire partition.
    eproc2g m01 MUST NOT overwrite — it would delete ejud rows from the same partition.
    Pattern: DELETE WHERE cod_sistema=2 AND meta_ano=X AND variavel=Y → APPEND.
    Orchestration requirement: ejud m01 must run BEFORE eproc2g m01.

Variables written per run (cod_sistema=2):
    cnm01        cod_indicador=1  Casos novos — first-ever distribution in [period_start, period_end]
    julgadom01   cod_indicador=2  cnm01 processes judged in the same period
    susm01       cod_indicador=3  cnm01 processes suspended at period_end (snapshot)
    desm01       cod_indicador=4  cnm01 processes unsuspended during the period (flow)
    PendJulg01TG cod_indicador=5  cnm01 LEFT ANTI JOIN julgadom01 (new cases not yet judged)

Eligible id_processo:
    All processes in tb_tf_situacao_eproc with any distribution event (situacao IN
    TRIGGER_CODES) up to period_end. No id_orgao_colegiado filter — the eproc2g system
    is 2nd-instance-only so every process in tb_tf_situacao_eproc is already eligible.

cnm01 algorithm (sit-88 based — official CNJ rule):
    1. Scan sit-88 + trigger codes [24, 9, 65, 81] from DIST_START to period_end.
       Note: eproc2g uses sit-24 only — sit-61 does NOT exist in eproc2g data.
    2. First sit-88 per id_processo (ASC dth_inicio).
    3. Trigger validation: at least one trigger event with dth_inicio <= first_88_dt.
    4. Keep processes where first_88_dt IN [period_start, period_end].

julgadom01:
    ALL eligible processes judged in period — NOT restricted to cnm01 (counts backlog too).
    union(primeirasentm + baixm) → first per id_processo in period, enriched from bronze.
    primeirasentm: sit IN [27, 28, 62, 18, 29, 90, 129]
    baixm:         sit IN [2, 10, 23, 41, 134, 154, 132]
    (decm sit=140 and mpum sit=[17,34] not observed in tb_tf_situacao_eproc — excluded)

susm01 / desm01:
    Last suspension event (ADD or REMOVE) per id_processo up to period_end:
      - ADD code  → still suspended at period_end → susm01
      - REMOVE code AND in [period_start, period_end] → desm01

PendJulg01TG:
    cnm01 LEFT ANTI JOIN julgadom01 — new cases not yet judged in period.
    Computed, NOT read from situacao.
    Uses the full julgadom01 pool (all eligible judgments), so a cnm01 process
    judged in 2026 (even if distributed in prior years) is correctly excluded.

Enrichment from bronze:
    id_assunto_principal → assunto_judicial.cod_mni_assunto_judicial (internal ID → CNJ code)
    cod_competencia      → processo.cod_competencia
    cod_natureza         → competencia_judicial (sin_civel='S'→1, sin_criminal='S'→2)
                           NOTE: all eproc2g competencias have sin_civel='S', sin_criminal='N',
                           so cod_natureza = 1 (cível) for every eproc2g process.
    cod_ult_oj           → id_orgao_colegiado from situacao (already available, no extra join)
    cod_ult_magistrado   → NULL (not available without log_processo / historico_distribuicao join)

dth_evento_indicador per variable:
    cnm01        → dth_inicio of distribution event
    julgadom01   → dth_inicio of first judgment event in period
    PendJulg01TG → dth_inicio of distribution event (same as cnm01)
    susm01       → dth_inicio of distribution event (snapshot — process is in cnm01 pool)
    desm01       → dth_last_susp (when suspension was lifted)

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/scripts/eproc_metacnj_m01_emr.py \\
      --year 2026 --period-start 2026-01-01 --period-end 2026-12-31 2>&1 | \\
      grep --line-buffered -E 'eproc_metacnj_m01|ERROR|Exception'

Validate in Athena:
    SELECT variavel, COUNT(*) AS n
    FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = 2026 AND cod_sistema = 2
    GROUP BY cod_indicador ORDER BY cod_indicador;
"""

import argparse
import json
import logging
import sys
from datetime import date
from typing import List, Optional

import boto3

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    broadcast, coalesce, col, current_timestamp, lit, lpad, regexp_replace,
    row_number, to_timestamp, when,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("eproc_metacnj_m01_emr")


# ── Config loader ─────────────────────────────────────────────────────────────
_DEFAULT_CONFIG_PATH = (
    "s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/json/meta_config.json"
)

def _load_config(path: str) -> dict:
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Catalog / Iceberg constants ───────────────────────────────────────────────
# Production target is fixed in ouro; CLI can override for temp/test environments.
_CATALOG        = "glue_catalog"
_WAREHOUSE      = "s3://sgdai-data-lake"
_TARGET_DB      = "dw_2inst_ouro"
_TARGET_TABLE   = "tb_ft_meta_analitico"
_TARGET_LOC     = f"{_WAREHOUSE}/ouro/{_TARGET_DB}/{_TARGET_TABLE}"
_PARTITION_COLS = ["meta_ano", "variavel"]

# ── Source tables ─────────────────────────────────────────────────────────────
_DB_SITUACAO    = "dw_2inst_ouro"
_SITUACAO_TABLE = "tb_tf_situacao_eproc"
_DB_BRONZE      = "eproc2g"


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EprocMetaCNJ_M01")
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
            num_processo          STRING,
            cod_sistema           INT,
            cod_classe_cnj        INT,
            id_assunto_principal  INT,
            cod_competencia       INT,
            dth_evento_indicador  TIMESTAMP,
            cod_indicador         INT,
            id_sigilo             INT,
            cod_natureza          INT,
            dth_atualizacao       TIMESTAMP,
            cod_ult_oj            DECIMAL(38,18),
            cod_ult_magistrado    DECIMAL(38,18),
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


def _save_eproc(
    spark: SparkSession,
    df: DataFrame,
    cod_ind: int,
    sistema: int,
    meta_ano: str,
    variavel: str = "",
) -> None:
    """
    Write eproc2g (cod_sistema=2) rows to tb_ft_meta_analitico.

    Uses DELETE + APPEND instead of overwritePartitions() so that ejud
    (cod_sistema=1) rows in the same meta_ano partition are preserved.
    DELETE targets the specific (cod_sistema, meta_ano, cod_indicador) triplet.
    """
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    label = variavel or str(cod_ind)
    logger.info(
        "DELETE eproc2g rows: meta_ano=%s variavel=%s cod_sistema=%d cod_indicador=%d",
        meta_ano, label, sistema, cod_ind,
    )
    spark.sql(
        f"DELETE FROM {fqn} "
        f"WHERE cod_sistema = {sistema} "
        f"AND meta_ano = {int(meta_ano)} "
        f"AND cod_indicador = {cod_ind}"
    )
    logger.info("APPEND eproc2g rows: %s → %s", label, fqn)
    df.writeTo(fqn).append()
    logger.info("Write complete: %s", label)


# ── Calculator ────────────────────────────────────────────────────────────────
class MetaCNJM01EprocCalculator:
    """
    EPROC2G Meta 01 — cnm01, julgadom01, susm01, desm01, PendJulg01TG.

    Reads from dw_2inst_ouro.tb_tf_situacao_eproc.
    Enriches from eproc2g bronze tables (assunto_judicial, processo, competencia_judicial).
    Writes to dw_2inst_ouro.tb_ft_meta_analitico with cod_sistema=2 using DELETE+APPEND.
    """

    def __init__(
        self,
        spark:       SparkSession,
        cfg:         dict,
        catalog:     str = _CATALOG,
        db_situacao: str = _DB_SITUACAO,
        db_bronze:   str = _DB_BRONZE,
    ):
        self.spark       = spark
        self.cat         = catalog
        self.db_situacao = db_situacao
        self.db_bronze   = db_bronze

        cat_cfg = cfg["catalog"]
        sh      = cfg["shared"]
        sc      = sh["situacao_codes"]
        m1      = cfg["meta_01"]

        self.IND_SISTEMA          = cat_cfg["ind_sistema"]
        self.COD_INDICADOR_MAP    = m1["cod_indicador_map"]
        self.DIST_START           = date.fromisoformat(sh["dist_start"])
        self.COMPETENCIA_EXCLUDE  = sh["competencia_exclude"]

        # Trigger codes for cnm01 sit-88 approach (eproc2g: NO sit-61, only sit-24)
        self.CONHECIMENTO_DIST_CODES = m1["conhecimento_dist_codes"]   # [24]
        self.OUTROS_DIST_CODES       = m1["outros_dist_codes"]         # [9,65,81]
        self.TRIGGER_CODES           = self.CONHECIMENTO_DIST_CODES + self.OUTROS_DIST_CODES  # [24,9,65,81]

        self.PRIM_SENT_CODES         = sc["prim_sent"]
        self.BAIXA_CODES             = sc["baixa"]
        self.SUSPENSION_ADD_CODES    = sc["suspension_add"]
        self.SUSPENSION_REMOVE_CODES = sc["suspension_remove"]
        self.CONHECIMENTO_MNI_CODES  = m1["conhecimento_mni_codes"]

    def _tbl(self, db: str, table: str) -> str:
        return f"{self.cat}.{db}.{table}"

    # ── Core situacao reader ──────────────────────────────────────────────────
    def _sit(
        self,
        codes,
        date_from: date,
        date_to:   date,
        eligible_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """
        Load rows from tb_tf_situacao_eproc filtered by situacao codes and date range.
        Optional eligible_df semi-join restricts to eligible id_processo.
        Returns all columns from the situacao table.
        """
        sit_tbl = self._tbl(self.db_situacao, _SITUACAO_TABLE)
        if isinstance(codes, int):
            codes = [codes]

        # Cast Python date → timestamp string to avoid DATE/TIMESTAMP implicit-cast
        # edge cases in different Spark versions (lit(date) → DateType midnight).
        ts_from = to_timestamp(lit(str(date_from)))   # "1988-01-01" → TS 00:00:00
        ts_to   = to_timestamp(lit(str(date_to) + " 23:59:59"))  # inclusive end-of-day

        df = (
            self.spark.table(sit_tbl)
            .filter(col("situacao").isin(codes))
            .filter(col("dth_inicio") >= ts_from)
            .filter(col("dth_inicio") <= ts_to)
            .select(
                "id_processo",
                "dth_inicio",
                "situacao",
                "id_classe_judicial",
                "id_assunto_principal",
                "id_orgao_juizo",
                "id_orgao_colegiado",
                "id_sigilo",
                "cod_mni_movimento",
                "num_processo",
            )
        )
        if eligible_df is not None:
            df = df.join(
                eligible_df.select("id_processo").distinct(),
                on="id_processo",
                how="semi",
            )
        return df

    # ── Bronze enrichment ─────────────────────────────────────────────────────
    def _enrich_from_bronze(self, df: DataFrame) -> DataFrame:
        """
        Enrich df (which must have id_processo and id_assunto_principal) with:
          1. assunto_judicial → id_assunto_principal resolved to CNJ code
          2. processo         → cod_competencia
          3. competencia_judicial → cod_natureza (sin_civel='S'→1, sin_criminal='S'→2)

        Each enrichment is wrapped in try/except — failure adds NULL columns
        so _finalize can safely reference them.
        """
        # 1. Resolve id_assunto_principal: internal eproc2g ID → CNJ code
        try:
            assunto = (
                self.spark.table(self._tbl(self.db_bronze, "assunto_judicial"))
                .select(
                    col("id_assunto_judicial"),
                    col("cod_mni_assunto_judicial").cast("int").alias("assunto_cnj"),
                )
                .distinct()
            )
            df = (
                df
                .join(
                    broadcast(assunto),
                    df["id_assunto_principal"] == assunto["id_assunto_judicial"],
                    how="left",
                )
                .drop("id_assunto_judicial", "id_assunto_principal")
                .withColumnRenamed("assunto_cnj", "id_assunto_principal")
            )
            logger.info("assunto_judicial enrichment OK")
        except Exception as exc:
            logger.warning("assunto_judicial enrichment skipped (%s) — keeping raw internal ID", exc)

        # 2 + 3. cod_competencia + cod_natureza via processo → competencia_judicial
        # Cast id_processo to STRING so the left-join key matches the STRING
        # id_processo that comes from tb_tf_situacao_eproc (df here has STRING type).
        try:
            processo = (
                self.spark.table(self._tbl(self.db_bronze, "processo"))
                .select(
                    col("id_processo").cast("string").alias("_proc_id"),
                    col("cod_competencia"),
                )
            )
            comp_jud = (
                self.spark.table(self._tbl(self.db_bronze, "competencia_judicial"))
                .select(
                    col("cod_competencia").alias("_comp_id"),
                    when(col("sin_civel")    == "S", lit(1))
                    .when(col("sin_criminal") == "S", lit(2))
                    .otherwise(lit(None).cast("int"))
                    .alias("cod_natureza"),
                )
            )
            proc_enriched = (
                processo
                .join(broadcast(comp_jud),
                      processo["cod_competencia"] == comp_jud["_comp_id"],
                      how="left")
                .drop("_comp_id")
            )
            df = (
                df
                .join(broadcast(proc_enriched),
                      df["id_processo"] == proc_enriched["_proc_id"],
                      how="left")
                .drop("_proc_id")
            )
            logger.info("processo / competencia_judicial enrichment OK")
        except Exception as exc:
            logger.warning("processo/competencia enrichment skipped (%s) — NULL cod_competencia/cod_natureza", exc)
            df = df.withColumn("cod_competencia", lit(None).cast("int"))
            df = df.withColumn("cod_natureza",    lit(None).cast("int"))

        return df

    # ── 0. Eligible processes ─────────────────────────────────────────────────
    def _build_eligible(self, period_end: date) -> DataFrame:
        """
        Three-step eligible filter (mirrors create_filtered_eproc_parquets.py):

        Step 1 — CONHECIMENTO class via hardcoded CONHECIMENTO_MNI_CODES:
            relacao_classe_judicial_mni WHERE cod_mni_classe_judicial IN CONHECIMENTO_MNI_CODES
              ↓ JOIN processo ON id_classe_judicial
            + exclude cod_competencia IN COMPETENCIA_EXCLUDE [1,2,9,2026]
            → eligible id_processo set (STRING — processo stores it as INT, cast here)

        Step 2 — Must have a distribution event in [DIST_START, period_end]:
            tb_tf_situacao_eproc WHERE situacao IN TRIGGER_CODES
              AND dth_inicio <= period_end

        Step 3 — Intersection (semi-join) of Step 1 and Step 2.

        Notes:
          - We do NOT join mni_classe_judicial for the natureza='Conhecimento' filter
            because the Iceberg table has incomplete natureza coverage — many historical
            and local class codes have NULL or missing natureza, causing eligible count to
            collapse to ~400 instead of the correct ~10 000+.
          - id_orgao_colegiado is the judging body (orgão julgador), NOT competencia.
            Available in eproc2g.processo (always up-to-date). Not used for eligibility.
          - id_processo cast: eproc2g.processo stores id_processo as INT/BIGINT while
            tb_tf_situacao_eproc stores it as STRING. Cast at Step 1 so all downstream
            joins operate on STRING↔STRING.
        """
        sit_tbl  = self._tbl(self.db_situacao, _SITUACAO_TABLE)
        proc_tbl = self._tbl(self.db_bronze, "processo")
        rcjm_tbl = self._tbl(self.db_bronze, "relacao_classe_judicial_mni")
        period_end_ts = to_timestamp(lit(str(period_end)))

        # Step 1a: internal id_classe_judicial values for Conhecimento CNJ codes.
        # Filter relacao_classe_judicial_mni directly — no mni_classe_judicial join.
        eligible_classes = (
            self.spark.table(rcjm_tbl)
            .filter(col("cod_mni_classe_judicial").isin(self.CONHECIMENTO_MNI_CODES))
            .select("id_classe_judicial")
            .distinct()
        )

        # Step 1b: processes with Conhecimento class + eligible competencia.
        # Cast id_processo INT/BIGINT → STRING to match tb_tf_situacao_eproc.id_processo.
        eligible_proc = (
            self.spark.table(proc_tbl)
            .join(broadcast(eligible_classes), on="id_classe_judicial", how="semi")
            .filter(~col("cod_competencia").isin(self.COMPETENCIA_EXCLUDE))
            .select(col("id_processo").cast("string").alias("id_processo"))
            .distinct()
        )

        # Step 2: processes with any distribution event up to period_end.
        # id_processo is already STRING in tb_tf_situacao_eproc.
        has_dist = (
            self.spark.table(sit_tbl)
            .filter(col("situacao").isin(self.TRIGGER_CODES))
            .filter(col("dth_inicio") <= period_end_ts)
            .select("id_processo")
            .distinct()
        )

        # Step 3: intersection — both sides STRING
        return eligible_proc.join(has_dist, on="id_processo", how="semi")

    # ── 1. cnm01 ──────────────────────────────────────────────────────────────
    def build_cnm01(
        self, period_start: date, period_end: date, eligible: DataFrame
    ) -> DataFrame:
        """
        CNM01 — official CNJ rule (sit-88 based).

        A process is a "caso novo" for the period if:
          1. Its FIRST-EVER sit-88 (Pendente) falls IN [period_start, period_end].
          2. That sit-88 was triggered by a distribution code (24, 9, 65, 81).
             Note: eproc2g has sit-24 only — sit-61 does NOT exist in eproc2g data.
          3. id_processo is in the eligible set.

        Single-scan optimisation: load sit-88 + trigger codes together, then split.
        """
        logger.info("Building cnm01 (sit-88 based): %s → %s", period_start, period_end)

        trigger_codes = self.TRIGGER_CODES  # [24, 9, 65, 81] — NO sit-61 for eproc2g

        # Step 1: one scan — sit-88 + trigger events up to period_end
        relevant = self._sit([88] + trigger_codes, self.DIST_START, period_end, eligible_df=eligible)
        relevant.cache()

        # Step 2: first sit-88 per id_processo
        w = Window.partitionBy("id_processo").orderBy(col("dth_inicio").asc())
        first_88 = (
            relevant.filter(col("situacao") == 88)
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .withColumnRenamed("dth_inicio", "first_88_dt")
        )

        # Step 3: trigger validation — trigger_dt <= first_88_dt
        triggers = (
            relevant.filter(col("situacao").isin(trigger_codes))
            .select("id_processo", col("dth_inicio").alias("trigger_dt"))
        )
        valid_88 = (
            first_88
            .join(triggers, on="id_processo", how="inner")
            .filter(col("trigger_dt") <= col("first_88_dt"))
            .dropDuplicates(["id_processo"])
            .drop("trigger_dt")
        )
        relevant.unpersist()

        # Step 4: first_88_dt IN [period_start, period_end]
        ps_ts = to_timestamp(lit(str(period_start)))
        pe_ts = to_timestamp(lit(str(period_end) + " 23:59:59"))
        return (
            valid_88
            .filter(col("first_88_dt") >= ps_ts)
            .filter(col("first_88_dt") <= pe_ts)
            .withColumnRenamed("first_88_dt", "dth_inicio")
        )

    # ── 2. julgadom01 ────────────────────────────────────────────────────────
    def build_julgadom01(
        self, eligible: DataFrame, period_start: date, period_end: date
    ) -> tuple:
        """
        ALL eligible processes judged in period — NOT restricted to cnm01.

        julgadom01 = primeirasentm ∪ baixm restricted to eligible pool,
        first occurrence per process in [period_start, period_end].

          primeirasentm  sit IN PRIM_SENT_CODES  [27,28,62,18,29,90,129]
          baixm          sit IN BAIXA_CODES      [2,10,23,41,134,154,132]

        decm (140) and mpum (17, 34) not observed in tb_tf_situacao_eproc — excluded.

        Meta 01 logic:
          julgadom01 counts every eligible case judged in the period regardless of
          when it was first distributed (may include backlog from prior years).
          PendJulg01TG = cnm01 LEFT ANTI JOIN julgadom01 — new cases not yet judged.

        Returns (julgadom_raw_df, judged_ids_df) where:
          - julgadom_raw_df  full situacao row (id_processo, dth_inicio, all _sit cols)
                             caller must call _enrich_from_bronze() before _finalize()
          - judged_ids_df    lightweight id_processo set for PendJulg01TG anti-join
        """
        logger.info("Building julgadom01: period=%s → %s", period_start, period_end)

        prim = self._sit(self.PRIM_SENT_CODES, period_start, period_end, eligible_df=eligible)
        baix = self._sit(self.BAIXA_CODES,     period_start, period_end, eligible_df=eligible)

        all_events = prim.unionByName(baix, allowMissingColumns=True)

        # First judgment event per process in the period (keeps full situacao row)
        w = Window.partitionBy("id_processo").orderBy(col("dth_inicio").asc())
        first_julg = (
            all_events
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        judged_ids = first_julg.select("id_processo").distinct()
        return first_julg, judged_ids

    # ── 3. susm01 / desm01 (suspension state) ────────────────────────────────
    def build_suspension_state(
        self, cnm01_enriched: DataFrame, period_start: date, period_end: date
    ) -> DataFrame:
        """
        Last suspension-related event (ADD or REMOVE) per id_processo up to period_end.
        Returns: id_processo, last_susp_situacao INT, dth_last_susp TIMESTAMP
        """
        logger.info("Building suspension state up to %s", period_end)
        all_susp = self.SUSPENSION_ADD_CODES + self.SUSPENSION_REMOVE_CODES
        w = Window.partitionBy("id_processo").orderBy(col("dth_inicio").desc())
        return (
            self._sit(all_susp, self.DIST_START, period_end, eligible_df=cnm01_enriched)
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select(
                col("id_processo"),
                col("situacao").alias("last_susp_situacao"),
                col("dth_inicio").alias("dth_last_susp"),
            )
        )

    # ── 4. _finalize ──────────────────────────────────────────────────────────
    def _finalize(
        self, df: DataFrame, variavel: str, meta_ano: str, event_col: str = "dth_inicio"
    ) -> DataFrame:
        """
        Map to the exact tb_ft_meta_analitico schema with cod_sistema=2.

        eproc2g column mapping:
          id_processo           ← id_processo (STRING, already correct)
          num_processo          ← num_processo
          cod_classe_cnj        ← id_classe_judicial (already CNJ MNI code from situacao join)
          id_assunto_principal  ← cod_mni_assunto_judicial (resolved by _enrich_from_bronze)
          cod_competencia       ← from processo (added by _enrich_from_bronze)
          dth_evento_indicador  ← event_col (distribution time for cnm01/susm01/PendJulg01TG;
                                             judgment time for julgadom01;
                                             dth_last_susp for desm01)
          id_sigilo             ← id_sigilo
          cod_natureza          ← from competencia_judicial (added by _enrich_from_bronze)
          cod_ult_oj            ← id_orgao_colegiado (from situacao, no extra join needed)
          cod_ult_magistrado    ← NULL (not available without historico_distribuicao join)
        """
        cod_ind: Optional[int] = self.COD_INDICADOR_MAP.get(variavel)

        # Guard: add enrichment columns as NULL if missing.
        for colname in ("cod_competencia", "cod_natureza", "id_assunto_principal"):
            if colname not in df.columns:
                df = df.withColumn(colname, lit(None).cast("int"))

        return (
            df
            .select(
                col("id_processo"),
                col("num_processo").cast("string").alias("num_processo"),
                lit(self.IND_SISTEMA).cast("int").alias("cod_sistema"),
                col("id_classe_judicial").cast("int").alias("cod_classe_cnj"),
                col("id_assunto_principal").cast("int"),
                col("cod_competencia").cast("int"),
                col(event_col).alias("dth_evento_indicador"),
                lit(cod_ind).cast("int").alias("cod_indicador"),
                coalesce(col("id_sigilo").cast("int"), lit(0)).alias("id_sigilo"),
                col("cod_natureza").cast("int"),
                current_timestamp().alias("dth_atualizacao"),
                col("id_orgao_colegiado").cast("decimal(38,18)").alias("cod_ult_oj"),
                lit(None).cast("decimal(38,18)").alias("cod_ult_magistrado"),
                lit(int(meta_ano)).cast("int").alias("meta_ano"),
            )
        )

    # ── 5. run ────────────────────────────────────────────────────────────────
    def run(self, year: int, period_start: date, period_end: date) -> None:
        meta_ano = str(year)

        logger.info("=" * 70)
        logger.info(
            "eproc_metacnj_m01_emr  year=%d  period=%s → %s",
            year, period_start, period_end,
        )
        logger.info("=" * 70)

        # Ensure target table exists once — _save_eproc skips this DDL call now.
        _ensure_target_table(self.spark)

        # ── Step 0: eligible processes ───────────────────────────────────────
        eligible = self._build_eligible(period_end)
        eligible.cache()
        n_elig = eligible.count()
        logger.info("eligible cached — count=%d", n_elig)

        if n_elig == 0:
            logger.warning("DIAGNOSTIC — n_elig=0, investigating root cause...")
            try:
                sit_tbl  = self._tbl(self.db_situacao, _SITUACAO_TABLE)
                proc_tbl = self._tbl(self.db_bronze, "processo")

                # A) distribution codes present in situacao?
                logger.warning("DIAG A — situacao IN TRIGGER_CODES counts:")
                self.spark.table(sit_tbl) \
                    .filter(col("situacao").isin(self.TRIGGER_CODES)) \
                    .groupBy("situacao").count().orderBy("situacao") \
                    .show(10, truncate=False)

                # B) how many id_classe_judicial match CONHECIMENTO_MNI_CODES in rcjm?
                rcjm_tbl = self._tbl(self.db_bronze, "relacao_classe_judicial_mni")
                n_eligible_classes = (
                    self.spark.table(rcjm_tbl)
                    .filter(col("cod_mni_classe_judicial").isin(self.CONHECIMENTO_MNI_CODES))
                    .select("id_classe_judicial").distinct().count()
                )
                logger.warning("DIAG B — eligible id_classe_judicial from rcjm: %d", n_eligible_classes)

                # C) how many processo rows match eligible classes + competencia filter?
                n_eligible_proc = (
                    self.spark.table(proc_tbl)
                    .join(
                        self.spark.table(rcjm_tbl)
                        .filter(col("cod_mni_classe_judicial").isin(self.CONHECIMENTO_MNI_CODES))
                        .select("id_classe_judicial").distinct(),
                        on="id_classe_judicial", how="semi",
                    )
                    .filter(~col("cod_competencia").isin(self.COMPETENCIA_EXCLUDE))
                    .count()
                )
                logger.warning("DIAG C — eligible processo rows (pre dist filter): %d", n_eligible_proc)

                # D) competencia distribution in bronze
                logger.warning("DIAG D — cod_competencia distribution in eproc2g.processo:")
                self.spark.table(proc_tbl) \
                    .groupBy("cod_competencia").count() \
                    .orderBy(col("count").desc()) \
                    .show(20, truncate=False)

                # E) how many have a distribution event?
                n_has_dist = (
                    self.spark.table(sit_tbl)
                    .filter(col("situacao").isin(self.TRIGGER_CODES))
                    .filter(col("dth_inicio") <= to_timestamp(lit(str(period_end))))
                    .select("id_processo").distinct().count()
                )
                logger.warning("DIAG E — processes with distribution event (STRING id): %d", n_has_dist)

            except Exception as _exc:
                logger.warning("DIAGNOSTIC failed: %s", _exc)
            logger.error("No eligible processes found — aborting run.")
            return

        # ── Step 1: cnm01 raw (sit-88 based) ────────────────────────────────
        cnm01_raw = self.build_cnm01(period_start, period_end, eligible)

        # ── Step 2: enrich cnm01 with bronze tables ───────────────────────────
        # Count BEFORE unpersisting eligible:
        #   - cnm01_raw references eligible in its lineage
        #   - eligible also needed by julgadom01 (Step 3) — defer unpersist
        n_cnm01_raw = cnm01_raw.count()
        logger.info("cnm01 raw (pre-enrich) — count=%d", n_cnm01_raw)

        if n_cnm01_raw == 0:
            logger.warning(
                "DIAGNOSTIC — cnm01 raw is 0 even though eligible=%d. "
                "Check DIST_START (%s) vs first sit-88 dates.",
                n_elig, self.DIST_START,
            )
            try:
                sit_tbl = self._tbl(self.db_situacao, _SITUACAO_TABLE)
                from pyspark.sql.functions import year as _yr
                logger.warning("DIAGNOSTIC — sit-88 + trigger situacoes by year for eligible processes:")
                (
                    self.spark.table(sit_tbl)
                    .join(eligible, on="id_processo", how="semi")
                    .filter(col("situacao").isin([88] + self.TRIGGER_CODES))
                    .groupBy(_yr(col("dth_inicio")).alias("yr"))
                    .count()
                    .orderBy("yr")
                    .show(20, truncate=False)
                )
            except Exception as _exc:
                logger.warning("DIAGNOSTIC failed: %s", _exc)

        cnm01 = self._enrich_from_bronze(cnm01_raw)
        cnm01.cache()
        n_cnm01 = cnm01.count()
        logger.info("cnm01 enriched + cached — count=%d", n_cnm01)

        _save_eproc(self.spark, self._finalize(cnm01, "cnm01", meta_ano), cod_ind=self.COD_INDICADOR_MAP["cnm01"], sistema=self.IND_SISTEMA, meta_ano=meta_ano, variavel="cnm01")
        logger.info("cnm01 written — count=%d", n_cnm01)

        # ── Step 3: julgadom01 ───────────────────────────────────────────────
        # ALL eligible processes judged in period — NOT restricted to cnm01.
        # eligible is still cached here; unpersist after julgadom_raw is materialised.
        julgadom_raw, judged_ids = self.build_julgadom01(eligible, period_start, period_end)
        n_julgadom_raw = julgadom_raw.count()
        eligible.unpersist()   # both cnm01_raw and julgadom_raw now materialised
        logger.info("julgadom01 raw (pre-enrich) — count=%d", n_julgadom_raw)

        julgadom = self._enrich_from_bronze(julgadom_raw)
        julgadom.cache()
        n_julgadom = julgadom.count()
        logger.info("julgadom01 enriched + cached — count=%d", n_julgadom)

        _save_eproc(
            self.spark,
            self._finalize(julgadom, "julgadom01", meta_ano, event_col="dth_inicio"),
            cod_ind=self.COD_INDICADOR_MAP["julgadom01"],
            sistema=self.IND_SISTEMA,
            meta_ano=meta_ano,
            variavel="julgadom01",
        )
        logger.info("julgadom01 written — count=%d", n_julgadom)

        # ── Step 4: PendJulg01TG = cnm01 anti-join julgadom01 ────────────────
        pendjulg = cnm01.join(judged_ids, on="id_processo", how="left_anti")
        n_pendjulg = pendjulg.count()
        logger.info("PendJulg01TG — count=%d", n_pendjulg)

        _save_eproc(
            self.spark,
            self._finalize(pendjulg, "PendJulg01TG", meta_ano),
            cod_ind=self.COD_INDICADOR_MAP["PendJulg01TG"],
            sistema=self.IND_SISTEMA,
            meta_ano=meta_ano,
            variavel="PendJulg01TG",
        )
        logger.info("PendJulg01TG written — count=%d", n_pendjulg)
        julgadom.unpersist()

        # ── Step 5: suspension state (used for both susm01 and desm01) ───────
        susp = self.build_suspension_state(cnm01, period_start, period_end)

        # susm01: last suspension event is an ADD code → still suspended at period_end
        susm_ids = (
            susp
            .filter(col("last_susp_situacao").isin(self.SUSPENSION_ADD_CODES))
            .select("id_processo")
            .distinct()
        )
        n_susm = susm_ids.count()
        logger.info("susm01 — count=%d", n_susm)
        _save_eproc(
            self.spark,
            self._finalize(cnm01.join(susm_ids, on="id_processo", how="semi"), "susm01", meta_ano),
            cod_ind=self.COD_INDICADOR_MAP["susm01"],
            sistema=self.IND_SISTEMA,
            meta_ano=meta_ano,
            variavel="susm01",
        )
        logger.info("susm01 written — count=%d", n_susm)

        # desm01: last suspension event is a REMOVE code AND happened in the period
        ps_ts = to_timestamp(lit(str(period_start)))
        pe_ts = to_timestamp(lit(str(period_end) + " 23:59:59"))
        desm_raw = (
            susp
            .filter(col("last_susp_situacao").isin(self.SUSPENSION_REMOVE_CODES))
            .filter(col("dth_last_susp") >= ps_ts)
            .filter(col("dth_last_susp") <= pe_ts)
        )
        desm_ids = desm_raw.select("id_processo").distinct()
        # Join cnm01 enrichment cols + carry dth_last_susp as the event time
        desm_df = (
            cnm01.join(desm_ids, on="id_processo", how="semi")
                 .join(
                     desm_raw.select("id_processo", "dth_last_susp"),
                     on="id_processo", how="left",
                 )
        )
        n_desm = desm_df.count()
        logger.info("desm01 — count=%d", n_desm)
        _save_eproc(
            self.spark,
            self._finalize(desm_df, "desm01", meta_ano, event_col="dth_last_susp"),
            cod_ind=self.COD_INDICADOR_MAP["desm01"],
            sistema=self.IND_SISTEMA,
            meta_ano=meta_ano,
            variavel="desm01",
        )
        logger.info("desm01 written — count=%d", n_desm)

        cnm01.unpersist()

        logger.info(
            "Summary — cnm01=%d  julgadom01=%d  PendJulg01TG=%d  susm01=%d  desm01=%d",
            n_cnm01, n_julgadom, n_pendjulg, n_susm, n_desm,
        )
        logger.info("=" * 70)
        logger.info("eproc_metacnj_m01_emr  DONE  year=%d", year)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    today = date.today()
    p = argparse.ArgumentParser(
        description="EPROC2G Meta 01 indicators (cod_sistema=2) → Iceberg dw_2inst_ouro.tb_ft_meta_analitico"
    )
    p.add_argument(
        "--year", type=int, default=today.year,
        help="Reference year (default: current year). Used as meta_ano STRING.",
    )
    p.add_argument(
        "--period-start", type=date.fromisoformat,
        default=date(today.year, 1, 1),
        help="Period start (ISO format, default: Jan 1 of --year)",
    )
    p.add_argument(
        "--period-end", type=date.fromisoformat,
        default=date(today.year, 12, 31),
        help="Period end (ISO format, default: Dec 31 of --year)",
    )
    p.add_argument("--config-path", default=_DEFAULT_CONFIG_PATH,
                   help="Path to meta_config.json (local or s3://...)")
    p.add_argument("--catalog",                  default=_CATALOG)
    p.add_argument("--db-situacao",              default=_DB_SITUACAO,
                   help="Database with tb_tf_situacao_eproc (default: dw_2inst_ouro)")
    p.add_argument("--db-bronze",                default=_DB_BRONZE,
                   help="eproc2g bronze database for enrichment (default: eproc2g)")
    p.add_argument(
        "--target-db", default=None,
        help="Glue database for target table (default: from meta_config.json). "
             "Use 'temp_2inst_ouro' for pre-production testing.",
    )
    p.add_argument(
        "--target-loc", default=None,
        help="S3 location for target Iceberg table (default: from meta_config.json). "
             "Override when writing to a temp database.",
    )
    p.add_argument("--executor-memory",          default="12g")
    p.add_argument("--executor-memory-overhead", default="2g")
    p.add_argument("--shuffle-partitions", type=int, default=200)
    return p.parse_args(argv)


def main(argv=None):
    args  = _parse_args(argv)
    cfg   = _load_config(args.config_path)

    # Keep production writes in ouro even if the deployed config is stale.
    global _TARGET_DB, _TARGET_TABLE, _TARGET_LOC  # noqa: PLW0603
    cat_cfg = cfg["catalog"]
    if (
        cat_cfg.get("target_db") != _TARGET_DB
        or cat_cfg.get("target_table") != _TARGET_TABLE
        or cat_cfg.get("target_loc") != _TARGET_LOC
    ):
        logger.warning(
            "Config target differs from production default; using %s.%s at %s unless CLI override is provided.",
            _TARGET_DB,
            _TARGET_TABLE,
            _TARGET_LOC,
        )

    _TARGET_DB  = args.target_db or _TARGET_DB
    _TARGET_LOC = args.target_loc or _TARGET_LOC

    logger.info(
        "Resolved target table: %s.%s.%s at %s",
        args.catalog,
        _TARGET_DB,
        _TARGET_TABLE,
        _TARGET_LOC,
    )

    spark = _create_spark_session(
        args.executor_memory,
        args.executor_memory_overhead,
        args.shuffle_partitions,
    )
    calc = MetaCNJM01EprocCalculator(
        spark=spark,
        cfg=cfg,
        catalog=args.catalog,
        db_situacao=args.db_situacao,
        db_bronze=args.db_bronze,
    )
    try:
        calc.run(year=args.year, period_start=args.period_start, period_end=args.period_end)
    except Exception as exc:
        logger.error("Job failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
