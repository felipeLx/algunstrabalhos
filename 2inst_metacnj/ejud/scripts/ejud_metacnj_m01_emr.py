#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_metacnj_m01_emr.py — EJUD Meta 01 — indicators → Iceberg (ouro layer)

Single-script design: no external .py dependencies.
All data reads via spark.table() from the Glue catalog.

Source   : glue_catalog.temp_2inst_ouro.tb_tf_situacao_ejud
           s3://sgdai-data-lake/ouro/dw_2inst_ouro/tb_tf_situacao_ejud
           Partitioned by month(dthrmov)
           Columns used: coddoc, dthrmov, situacao, cod_classe (STRING), cod_oj (STRING), nivel_sigilo
           Bronze: glue_catalog.ejud.tipoprocesso, glue_catalog.ejud.t_processo

Target   : glue_catalog.dw_2inst_prata.tb_2inst_metacnj
           s3://sgdai-data-lake/prata/dw_2inst_prata/tb_2inst_metacnj
           Partitioned by (meta_ano, variavel)

Variables written per run:
    cnm01       Casos novos — true first-ever start in [period_start, period_end]
    julgadom01  cnm01 processes judged in the same period
    susm01      cnm01 processes suspended at period_end (snapshot)
    desm01      cnm01 processes unsuspended during the period (flow)

Eligible coddoc (conhecimento + outros):
    tipoprocesso(codextcnj IN CLASSE_CONHECIMENTO AND (codnat=1 OR NULL))  → cível
    UNION
    tipoprocesso(codextcnj IN CLASSE_OUTROS)                               → criminal
    → distinct codtipproc
    + competencia(codcompt NOT IN [9,12,21])   ← INNER JOIN via t_processo.codcompt
    → INNER JOIN t_processo on codtipproc + codcompt → distinct coddoc
    + documento(indativo = 'S')                   ← SEMI JOIN (active documents only)

cnm01 algorithm (sit-88 based — official CNJ rule):
    1. Scan sit-88 (Pendente) + trigger codes [61,24,9,65,81] from DIST_START to period_end.
    2. First sit-88 per coddoc (ASC dthrmov).
    3. Validate: at least one trigger event with dthrmov <= first_88_dt.
    4. Semi-join on eligible coddoc.
    5. Keep coddocs where first_88_dt IN [period_start, period_end].
    CNJ rule: a process is "novo" for the period when its first Pendente status
    was initiated by a distribution event within the reporting window.

julgadom01:
    union(primeirasentm01 + baixm01 + mpum01) → first judgment per coddoc in period
    primeirasentm01: sit IN [27,28,62,18,29,90,129]
    baixm01:         sit IN [2,10,23,41,134,154,132]
    mpum01:          sit IN [17,34] restricted to MPU_VALID_MOV_CODES

susm01 / desm01:
    Last suspension event (ADD or REMOVE) per coddoc up to period_end:
      - ADD code  → still suspended → susm01
      - REMOVE code AND in period → unsuspended this period → desm01

Usage:
    spark-submit --master yarn --deploy-mode client \
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
      s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts/ejud_metacnj_m01_emr.py \
      --year 2026 --period-start 2026-01-01 --period-end 2026-12-31  2>&1 | \
      grep --line-buffered -E 'ejud_metacnj_m01|ERROR|Exception'

Validate in Athena:
    SELECT variavel, COUNT(*) AS n
    FROM temp_2inst_ouro.tb_2inst_metacnj
    WHERE meta_ano = '2026' AND cd_meta = '01' AND cod_sistema = 1
    GROUP BY variavel ORDER BY variavel;
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
    broadcast, coalesce, col, concat, current_timestamp, lit, lpad,
    regexp_replace, row_number, upper, when,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_metacnj_m01_emr")


# ── Config loader ─────────────────────────────────────────────────────────────
_DEFAULT_CONFIG_PATH = (
    "s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/json/meta_config.json"
)

def _load_config(path: str) -> dict:
    """Load meta_config.json from S3 or local path."""
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Catalog / Iceberg constants ───────────────────────────────────────────────
# Defaults loaded from meta_config.json at runtime; can be overridden via CLI.
_CATALOG        = "glue_catalog"
_WAREHOUSE      = "s3://sgdai-data-lake"
_TARGET_DB      = None   # resolved from config["catalog"]["target_db"]
_TARGET_TABLE   = None   # resolved from config["catalog"]["target_table"]
_TARGET_LOC     = None   # resolved from config["catalog"]["target_loc"]
_PARTITION_COLS = ["meta_ano", "variavel"]

# ── Source tables ─────────────────────────────────────────────────────────────
_DB_SITUACAO    = "dw_2inst_ouro"
_SITUACAO_TABLE = "tb_tf_situacao_ejud"
# DDL reference columns used:
#   coddoc BIGINT, dthrmov TIMESTAMP, situacao INT,
#   cod_classe STRING, cod_oj STRING, nivel_sigilo INT, cod_tip_mov STRING
_DB_BRONZE      = "ejud"              # tipoprocesso, t_processo

# ── Business constants ────────────────────────────────────────────────────────
# All situacao codes, class codes, cod_indicador_map, competencia_exclude are
# loaded from meta_config.json via _load_config() and stored as self.XXX in
# MetaCNJM01EjudCalculator.__init__().

META_NUMBER = "01"

# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudMetaCNJ_M01")
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
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


# ── Iceberg table helper ──────────────────────────────────────────────────────
def _ensure_target_table(spark: SparkSession) -> None:
    """
    Create tb_2inst_metacnj if it does not yet exist.
    Uses the exact DDL agreed for this table — does not infer schema from DataFrame.
    """
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


def _save_to_iceberg(
    spark: SparkSession,
    df: DataFrame,
    cod_ind: int,
    sistema: int,
    meta_ano_int: int,
    variavel: str = "",
) -> None:
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    _ensure_target_table(spark)
    spark.sql(
        f"DELETE FROM {fqn} "
        f"WHERE cod_sistema = {sistema} "
        f"AND meta_ano = {meta_ano_int} "
        f"AND cod_indicador = {cod_ind}"
    )
    label = variavel or str(cod_ind)
    logger.info("Writing %s → %s", label, fqn)
    df.writeTo(fqn).append()
    logger.info("Write complete: %s", label)


# ── Utility ───────────────────────────────────────────────────────────────────
def _norm(c):
    """Strip trailing .0 from numeric-as-string columns (e.g. '12.0' → '12')."""
    return regexp_replace(c.cast("string"), r"\.0$", "")


def _first_per_coddoc(df: DataFrame, ts_col: str = "dthrmov") -> DataFrame:
    """Keep the row with the earliest ts_col per coddoc (row_number ASC)."""
    w = Window.partitionBy("coddoc").orderBy(col(ts_col).asc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


# ── Main calculator ───────────────────────────────────────────────────────────
class MetaCNJM01EjudCalculator:
    """
    EJUD Meta 01 — cnm01, julgadom01, susm01, desm01.

    Source DDL columns from tb_tf_situacao_ejud used here:
        coddoc BIGINT, dthrmov TIMESTAMP, situacao INT,
        cod_classe STRING, cod_oj STRING, nivel_sigilo INT

    Output maps to tb_ft_meta_analitico:
        id_processo          ← coddoc cast to STRING
        cod_classe_cnj       ← cod_classe cast to INT (strip trailing .0 if present)
        id_sigilo            ← t_processo flags only:
                                IndSuperSigil='S'→5, IndSigil='S'→4, IndSegrJust='S'→1, else 0
                                (nivel_sigilo from situacao is NOT used)
        num_processo         ← _enrich_t_processo (t_processo: CNJ 20-char format)
        cod_competencia      ← _enrich_t_processo (codcompt)
        cod_natureza         ← _enrich_t_processo (codnat via t_processo/tipoprocesso)
        id_assunto_principal ← _enrich_assunto (processoassunto.codass where numord=1)
        cod_ult_oj           ← _enrich_relator (movimentorelator.codorgjulg, last movement)
        cod_ult_magistrado   ← _enrich_relator (movimentorelator.codmagrel, last movement)
    """

    def __init__(
        self,
        spark: SparkSession,
        cfg: dict,
        catalog: str = None,
        db_situacao: str = None,
        db_bronze: str = None,
    ):
        cat_cfg = cfg["catalog"]
        sh      = cfg["shared"]
        sc      = sh["situacao_codes"]
        m1      = cfg["meta_01"]

        self.spark       = spark
        self.cat         = catalog     or cat_cfg["catalog"]
        self.db_situacao = db_situacao or cat_cfg["db_situacao"]
        self.db_bronze   = db_bronze   or cat_cfg["db_bronze"]

        # Identity
        self.IND_SISTEMA       = cat_cfg["ind_sistema"]
        self.COD_INDICADOR_MAP = m1["cod_indicador_map"]

        # Dates
        from datetime import date as _date
        self.DIST_START = _date.fromisoformat(sh["dist_start"])

        # Competencia
        self.COMPETENCIA_EXCLUDE = sh["competencia_exclude"]

        # Situacao code groups
        self.CONHECIMENTO_DIST_CODES  = m1["conhecimento_dist_codes"]
        self.OUTROS_DIST_CODES        = m1["outros_dist_codes"]
        self.PRIM_SENT_CODES          = sc["prim_sent"]
        self.BAIXA_CODES              = sc["baixa"]
        self.MPU_CODES                = sc["mpu"]
        self.MPU_VALID_MOV_CODES      = sc["mpu_valid_mov"]
        self.SUSPENSION_ADD_CODES     = sc["suspension_add"]
        self.SUSPENSION_REMOVE_CODES  = sc["suspension_remove"]
        self.DESM_COD_TIP_MOV         = sc["desm_cod_tip_mov"]
        self.SIT88_CLOSING_CODES      = sc["sit88_closing"]

        # Class code lists
        self.CLASSE_CONHECIMENTO          = sh["classe_conhecimento"]
        self.CLASSE_CONHECIMENTO_CRIMINAL = sh["classe_conhecimento_criminal"]
        self.CLASSE_OUTROS                = sh["classe_outros"]

    def _tbl(self, db: str, table: str) -> str:
        return f"{self.cat}.{db}.{table}"

    def _sit(
        self,
        codes: List[int],
        date_from: date,
        date_to: date,
        eligible_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """
        Load situacao events from tb_tf_situacao_ejud.
        Returns: coddoc, situacao, dthrmov, cod_classe (STRING), cod_oj (STRING)
        (nivel_sigilo is NOT read here — id_sigilo is derived from t_processo flags in _enrich)
        """
        df = (
            self.spark.table(self._tbl(self.db_situacao, _SITUACAO_TABLE))
            .filter(col("situacao").isin(codes))
            .filter(col("dthrmov") >= lit(date_from))
            .filter(col("dthrmov") <= lit(date_to))
            .select(
                col("coddoc"),
                col("situacao"),
                col("dthrmov"),
                col("cod_classe"),   # STRING — cast to INT in _finalize
                col("cod_oj"),       # STRING — dropped before _enrich_relator
                col("cod_tip_mov"),  # needed for MPU sit-17/34 filter
            )
        )
        if eligible_df is not None:
            df = df.join(
                eligible_df.select("coddoc").distinct(),
                on="coddoc",
                how="semi",
            )
        return df

    # ── Eligible coddoc ───────────────────────────────────────────────────────
    def build_eligible_coddoc(self) -> DataFrame:
        """
        Eligible coddoc: (classe conhecimento OR classe outros) ∩ valid competencia.

        Meta 01 includes BOTH conhecimento (civil) and outros (criminal) processes.
        The two class lists have zero overlap; codnat rules differ:
          - CLASSE_CONHECIMENTO: codnat = 1 OR NULL  (exclude criminal codnat=2)
          - CLASSE_OUTROS:       no codnat filter     (codnat=2 is valid here)

        Steps:
          1a. tipoprocesso WHERE codextcnj IN CLASSE_CONHECIMENTO
                AND (codnat=1 OR codnat IS NULL) → codtipproc
          1b. tipoprocesso WHERE codextcnj IN CLASSE_OUTROS → codtipproc (any codnat)
          1c. Union both → eligible_codtipproc
          2.  competencia WHERE codcompt NOT IN [1,9,12,21] → valid codcompt set
          3.  documento WHERE indativo = 'S' → active coddoc set
          4.  t_processo INNER JOIN codtipproc + codcompt; SEMI JOIN active_docs
              → distinct coddoc
        """
        logger.info("Building eligible coddoc (conhecimento civil(codnat=1/NULL) + criminal(codnat=2) + outros) inline")

        # Step 1a: conhecimento — civil (codnat=1/NULL) + criminal (codnat=2 for criminal subset)
        tp = self.spark.table(self._tbl(self.db_bronze, "tipoprocesso"))
        elig_civ = (
            tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_CONHECIMENTO))
            .filter((_norm(col("codnat")).cast("int") == 1) | col("codnat").isNull())
            .select("codtipproc").distinct()
        )
        elig_crim = (
            tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_CONHECIMENTO_CRIMINAL))
            .select("codtipproc").distinct()
        )
        elig_conhecimento = elig_civ.union(elig_crim).distinct()

        # Step 1b: outros — no codnat restriction (codnat=2 valid)
        elig_outros = (
            tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_OUTROS))
            .select("codtipproc").distinct()
        )

        # Step 1c: union
        eligible_codtipproc = elig_conhecimento.union(elig_outros).distinct()

        # Step 2: competencia — valid codcompt set (NOT IN 1,9,12,21)
        eligible_codcompt = (
            self.spark.table(self._tbl(self.db_bronze, "competencia"))
            .filter(~_norm(col("codcompt")).cast("int").isin(self.COMPETENCIA_EXCLUDE))
            .select(_norm(col("codcompt")).cast("int").alias("codcompt_int"))
            .distinct()
        )

        # Step 3: documento — active documents only
        active_docs = (
            self.spark.table(self._tbl(self.db_bronze, "documento"))
            .filter(col("indativo") == "S")
            .select("coddoc").distinct()
        )

        # Step 4: t_processo INNER JOINed on codtipproc + codcompt; SEMI-JOINed on active_docs
        return (
            self.spark.table(self._tbl(self.db_bronze, "t_processo"))
            .select(
                "coddoc",
                "codtipproc",
                _norm(col("codcompt")).cast("int").alias("codcompt_int"),
            )
            .join(broadcast(eligible_codtipproc), on="codtipproc", how="inner")
            .join(broadcast(eligible_codcompt), on="codcompt_int", how="inner")
            .join(active_docs, on="coddoc", how="semi")
            .select("coddoc")
            .distinct()
        )

    # ── cnm01 ─────────────────────────────────────────────────────────────────
    def build_cnm01(
        self,
        period_start: date,
        period_end: date,
        eligible: DataFrame,
    ) -> DataFrame:
        """
        CNM01 — official CNJ rule (sit-88 based).

        A process is a "caso novo" for the period if:
          1. Its FIRST-EVER sit-88 (Pendente) falls IN [period_start, period_end].
          2. That sit-88 was triggered by a distribution code (9, 24, 61, 65, 81):
             i.e. at least one trigger event exists with dthrmov <= first_88_dt.
          3. coddoc is in the eligible set.

        Single-scan optimisation: load sit-88 + trigger codes together, then split.
        """
        logger.info("Building cnm01 (sit-88 based): %s → %s", period_start, period_end)

        trigger_codes = self.CONHECIMENTO_DIST_CODES + self.OUTROS_DIST_CODES  # [61,24,9,65,81]

        # Step 1: one scan — sit-88 + trigger events up to period_end
        relevant = (
            self.spark.table(self._tbl(self.db_situacao, _SITUACAO_TABLE))
            .filter(col("situacao").isin([88] + trigger_codes))
            .filter(col("dthrmov") >= lit(self.DIST_START))
            .filter(col("dthrmov") <= lit(period_end))
            .select("coddoc", "situacao", "dthrmov", "cod_classe", "cod_oj", "cod_tip_mov")
        )
        relevant.cache()

        # Step 2: first sit-88 per coddoc
        w = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        first_88 = (
            relevant.filter(col("situacao") == 88)
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select(
                "coddoc",
                col("dthrmov").alias("first_88_dt"),
                "cod_classe",
                "cod_oj",
                "cod_tip_mov",
            )
        )

        # Step 3: trigger validation — trigger_dt <= first_88_dt
        triggers = (
            relevant.filter(col("situacao").isin(trigger_codes))
            .select("coddoc", col("dthrmov").alias("trigger_dt"))
        )
        valid_88 = (
            first_88
            .join(triggers, on="coddoc", how="inner")
            .filter(col("trigger_dt") <= col("first_88_dt"))
            .select("coddoc", "first_88_dt", "cod_classe", "cod_oj", "cod_tip_mov")
            .dropDuplicates(["coddoc"])
        )
        relevant.unpersist()

        # Step 4: eligible filter
        valid_88 = valid_88.join(
            eligible.select("coddoc").distinct(), on="coddoc", how="semi"
        )

        # Step 5: first_88_dt IN [period_start, period_end] → rename to dthrmov
        return (
            valid_88
            .filter(col("first_88_dt") >= lit(period_start))
            .filter(col("first_88_dt") <= lit(period_end))
            .withColumnRenamed("first_88_dt", "dthrmov")
        )

    # ── primeirasentm01 ───────────────────────────────────────────────────────
    def build_primeirasentm01(
        self,
        period_start: date,
        period_end: date,
        eligible: DataFrame,
    ) -> DataFrame:
        """
        primeirasentm01 — first sentence per cnm01 process in [period_start, period_end].
        Situacoes: PRIM_SENT_CODES [27, 28, 62, 18, 29, 90, 129]
        """
        logger.info("Building primeirasentm01: %s → %s", period_start, period_end)
        events = self._sit(self.PRIM_SENT_CODES, period_start, period_end, eligible_df=eligible)
        return _first_per_coddoc(events)

    # ── baixm01 ───────────────────────────────────────────────────────────────
    def build_baixm01(
        self,
        period_start: date,
        period_end: date,
        eligible: DataFrame,
    ) -> DataFrame:
        """
        baixm01 — first baixa (archive) per cnm01 process in [period_start, period_end].
        Situacoes: BAIXA_CODES [2, 10, 23, 41, 134, 154, 132]
        """
        logger.info("Building baixm01: %s → %s", period_start, period_end)
        events = self._sit(self.BAIXA_CODES, period_start, period_end, eligible_df=eligible)
        return _first_per_coddoc(events)

    # ── mpum01 ────────────────────────────────────────────────────────────────
    def build_mpum01(
        self,
        period_start: date,
        period_end: date,
        eligible: DataFrame,
    ) -> DataFrame:
        """
        mpum01 — first MPU/medida protetiva decision per cnm01 process in period.
        Situacoes: MPU_CODES [17, 34] restricted to MPU_VALID_MOV_CODES.
        """
        logger.info("Building mpum01: %s → %s", period_start, period_end)
        events = (
            self._sit(self.MPU_CODES, period_start, period_end, eligible_df=eligible)
            .filter(col("cod_tip_mov").cast("int").isin(self.MPU_VALID_MOV_CODES))
        )
        return _first_per_coddoc(events)

    # ── julgadom01 ────────────────────────────────────────────────────────────
    def build_julgadom01(
        self,
        prim_df: DataFrame,
        baixm_df: DataFrame,
        mpum_df: DataFrame,
    ) -> DataFrame:
        """
        julgadom01 — first judgment event per cnm01 process in period.

        Algorithm:
          1. Union primeirasentm01 + baixm01 + mpum01
          2. Apply sit-17 cod_tip_mov guard (belt-and-suspenders)
          3. FIRST per coddoc ordered by dthrmov asc
        """
        logger.info("Building julgadom01: union(primeirasentm01, baixm01, mpum01)")

        all_events = (
            prim_df.unionByName(baixm_df, allowMissingColumns=True)
                   .unionByName(mpum_df,  allowMissingColumns=True)
        )
        # Belt-and-suspenders: sit-17 MUST have valid cod_tip_mov
        all_events = all_events.filter(
            (col("situacao") != 17)
            | col("cod_tip_mov").cast("int").isin(self.MPU_VALID_MOV_CODES)
        )
        return _first_per_coddoc(all_events)

    # ── Suspension state ──────────────────────────────────────────────────────
    def build_susm01(
        self,
        pool_df: DataFrame,
        period_end: date,
    ) -> DataFrame:
        """
        susm01 — processes in pool_df whose last suspension event up to period_end
        is an ADD code (still suspended at snapshot date).

        pool_df: coddoc set to restrict to (Linux: cnm01 UNION desm01).
        Returns full event row (coddoc, dthrmov, situacao, cod_classe, cod_oj,
                cod_tip_mov) ready for _enrich / _finalize.
        """
        logger.info("Building susm01 up to %s", period_end)
        all_susp = self.SUSPENSION_ADD_CODES + self.SUSPENSION_REMOVE_CODES

        w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        return (
            self._sit(all_susp, self.DIST_START, period_end)
            .join(pool_df.select("coddoc").distinct(), on="coddoc", how="semi")
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .filter(col("situacao").isin(self.SUSPENSION_ADD_CODES))
        )

    # ── desm01 ────────────────────────────────────────────────────────────────
    def build_desm01(
        self,
        period_start: date,
        period_end: date,
        eligible_df: DataFrame,
        julgadom01_df: DataFrame,
    ) -> DataFrame:
        """
        desm01 — ALL eligible processes with levantamento de suspensão in period, not judged.

        Algorithm (situacao-based — NOT cod_tip_mov):
          The levantamento events in tb_tf_situacao_ejud are stored under their
          original EJUD situacao codes (20, 97–107, 138 = SUSPENSION_REMOVE_CODES).
          The cod_tip_mov MNI approach returns zero because those MNI codes are
          not populated for levantada events in this table.
          1. tb_tf_situacao_ejud in [period_start, period_end]
             filtered to situacao IN SUSPENSION_REMOVE_CODES
          2. SEMI JOIN on full eligible pool (NOT restricted to cnm01)
          3. ANTI JOIN against julgadom01 (exclude judged processes)
          4. First event per coddoc (earliest dthrmov)

        Note: Linux uses eligible processes (not cnm01) as the base pool.
        susm01 depends on desm01, so desm01 must be computed first.
        """
        logger.info("Building desm01 via situacao codes %s: %s → %s",
                    self.SUSPENSION_REMOVE_CODES, period_start, period_end)
        events = (
            self.spark.table(self._tbl(self.db_situacao, _SITUACAO_TABLE))
            .filter(col("dthrmov") >= lit(period_start))
            .filter(col("dthrmov") <= lit(period_end))
            .filter(col("situacao").isin(self.SUSPENSION_REMOVE_CODES))
            .select("coddoc", "dthrmov", "situacao", "cod_classe",
                    "cod_oj", "cod_tip_mov")
            .join(eligible_df.select("coddoc"), on="coddoc", how="semi")
            .join(julgadom01_df.select("coddoc"), on="coddoc", how="anti")
        )
        return _first_per_coddoc(events)

    # ── Enrichment ────────────────────────────────────────────────────────────

    def _enrich_t_processo(self, df: DataFrame) -> DataFrame:
        """
        Left-join ejud.t_processo to add:
          num_processo         — 20-char CNJ format (no separators)
          cod_competencia      — codcompt
          cod_natureza         — codnat from t_processo if present, otherwise from tipoprocesso
          nivel_sigilo_derived — DECODE(IndSuperSigil,'S',5, DECODE(IndSigil,'S',4,
                                   DECODE(IndSegrJust,'S',1,0)))
                                 always used as id_sigilo (situacao nivel_sigilo is ignored)
        """
        tp_raw = (
            self.spark.table(self._tbl(self.db_bronze, "t_processo"))
            .join(df.select("coddoc"), on="coddoc", how="semi")
        )
        tp_cols = [c.lower() for c in tp_raw.columns]

        num_proc_expr = concat(
            lpad(_norm(col("numsequencial")), 7, "0"),
            lit("-"),
            lpad(_norm(col("dv")),            2, "0"),
            lit("."),
            _norm(col("numano")),
            lit("."),
            _norm(col("codjustica")),
            lit("."),
            lpad(_norm(col("codregi")),    2, "0"),
            lit("."),
            lpad(_norm(col("codlocregi")), 4, "0"),
        )

        # Replicate: DECODE(IndSuperSigil,'S',5, DECODE(IndSigil,'S',4, DECODE(IndSegrJust,'S',1,0)))
        # Guard: columns may be absent in some clusters — fall back to 0 for missing ones.
        _HAS_SUPERSIGIL = "indsupersigil" in tp_cols
        _HAS_SIGIL      = "indsigil"      in tp_cols
        _HAS_SEGREJUST  = "indsegrejust"  in tp_cols
        if _HAS_SUPERSIGIL or _HAS_SIGIL or _HAS_SEGREJUST:
            _base = lit(0)
            if _HAS_SEGREJUST:
                _base = when(upper(col("indsegrejust")) == "S", lit(1)).otherwise(_base)
            if _HAS_SIGIL:
                _base = when(upper(col("indsigil")) == "S", lit(4)).otherwise(_base)
            if _HAS_SUPERSIGIL:
                _base = when(upper(col("indsupersigil")) == "S", lit(5)).otherwise(_base)
            sigilo_expr = _base.alias("nivel_sigilo_derived")
        else:
            logger.info("t_processo has no sigilo columns — nivel_sigilo_derived will be 0")
            sigilo_expr = lit(0).alias("nivel_sigilo_derived")

        if "codnat" in tp_cols:
            tp = tp_raw.select(
                col("coddoc"),
                num_proc_expr.alias("num_processo"),
                _norm(col("codcompt")).cast("int").alias("cod_competencia"),
                _norm(col("codnat")).cast("int").alias("cod_natureza"),
                sigilo_expr,
            )
        else:
            logger.info("t_processo has no codnat — joining tipoprocesso for cod_natureza")
            tipoproc = (
                self.spark.table(self._tbl(self.db_bronze, "tipoprocesso"))
                .select(
                    col("codtipproc"),
                    _norm(col("codnat")).cast("int").alias("cod_natureza"),
                )
                .distinct()
            )
            tp_base = tp_raw.select(
                col("coddoc"),
                num_proc_expr.alias("num_processo"),
                _norm(col("codcompt")).cast("int").alias("cod_competencia"),
                col("codtipproc"),
                sigilo_expr,
            )
            tp = (
                tp_base
                .join(broadcast(tipoproc), on="codtipproc", how="left")
                .drop("codtipproc")
            )

        return df.join(tp, on="coddoc", how="left")

    def _enrich_assunto(self, df: DataFrame) -> DataFrame:
        """Left-join ejud.processoassunto to add id_assunto_principal (numord=1, fallback to min numord)."""
        try:
            pa = (
                self.spark.table(self._tbl(self.db_bronze, "processoassunto"))
                .join(df.select("coddoc"), on="coddoc", how="semi")
                .select(
                    col("coddoc"),
                    _norm(col("numord")).cast("int").alias("numord"),
                    _norm(col("codass")).cast("int").alias("id_assunto_principal"),
                )
            )
            w = Window.partitionBy("coddoc").orderBy(col("numord").asc())
            assunto = (
                pa.withColumn("_rn", row_number().over(w))
                .filter(col("_rn") == 1)
                .select("coddoc", "id_assunto_principal")
            )
            return df.join(assunto, on="coddoc", how="left")
        except Exception as exc:
            logger.warning("Skipping assunto enrichment (%s)", exc)
            return df.withColumn("id_assunto_principal", lit(None).cast("int"))

    def _enrich_relator(self, df: DataFrame) -> DataFrame:
        """Left-join ejud.movimentorelator to add cod_relator + cod_oj (last movement up to period_end)."""
        try:
            relator_raw = (
                self.spark.table(self._tbl(self.db_bronze, "movimentorelator"))
                .join(df.select("coddoc"), on="coddoc", how="semi")
                .filter(col("dthrmov") <= lit(self._relator_cutoff))
                .select(
                    col("coddoc"),
                    col("dthrmov"),
                    _norm(col("codmagrel")).cast("int").alias("cod_relator"),
                    _norm(col("codorgjulg")).cast("int").alias("cod_oj"),
                )
            )
            # Exclude movements tied to cancelled distributions
            cancelled = (
                self.spark.table(self._tbl(self.db_bronze, "movcancedistr"))
                .select(
                    col("coddoc"),
                    col("dthrmovdistrcance").alias("dthrmov"),
                )
                .distinct()
            )
            relator_raw = relator_raw.join(cancelled, on=["coddoc", "dthrmov"], how="left_anti")
            w_rel = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
            relator = (
                relator_raw
                .withColumn("_rn", row_number().over(w_rel))
                .filter(col("_rn") == 1)
                .drop("_rn", "dthrmov")
            )
            return df.join(relator, on="coddoc", how="left")
        except Exception as exc:
            logger.warning("Skipping relator enrichment (%s)", exc)
            return (
                df.withColumn("cod_relator", lit(None).cast("int"))
                  .withColumn("cod_oj",      lit(None).cast("int"))
            )

    def _enrich(self, df: DataFrame) -> DataFrame:
        """
        Apply all enrichment steps (t_processo + assunto + relator).
        Drops situacao-sourced cod_oj before enrichment — cod_oj is re-added
        by _enrich_relator from ejud.movimentorelator (last movement).
        """
        if "cod_oj" in df.columns:
            df = df.drop("cod_oj")
        df = self._enrich_t_processo(df)
        df = self._enrich_assunto(df)
        df = self._enrich_relator(df)
        return df

    # ── Finalize → tb_ft_meta_analitico schema ────────────────────────────────
    def _finalize(
        self,
        df: DataFrame,
        variavel: str,
        meta_ano: str,
    ) -> DataFrame:
        """
        Map enriched DataFrame to the target schema of tb_ft_meta_analitico.

        Required input columns (from _sit): coddoc, dthrmov, cod_classe.
        Enriched columns (from _enrich): num_processo, cod_competencia, cod_natureza,
          id_assunto_principal, cod_relator, cod_oj, nivel_sigilo_derived.
        id_sigilo = nivel_sigilo_derived from t_processo flags
          (IndSuperSigil='S'→5, IndSigil='S'→4, IndSegrJust='S'→1, else 0).
        Any missing enriched column is added as NULL.
        """
        cod_ind: Optional[int] = self.COD_INDICADOR_MAP.get(variavel)

        for c_name, c_type in [
            ("num_processo",         "string"),
            ("cod_competencia",      "int"),
            ("cod_natureza",         "int"),
            ("id_assunto_principal", "int"),
            ("cod_relator",          "int"),
            ("cod_oj",               "int"),
            ("nivel_sigilo_derived", "int"),
        ]:
            if c_name not in df.columns:
                df = df.withColumn(c_name, lit(None).cast(c_type))

        return (
            df
            .select(
                col("coddoc").cast("string").alias("id_processo"),
                col("num_processo"),
                lit(self.IND_SISTEMA).cast("int").alias("cod_sistema"),
                _norm(col("cod_classe")).cast("int").alias("cod_classe_cnj"),
                col("id_assunto_principal"),
                col("cod_competencia"),
                col("dthrmov").alias("dth_evento_indicador"),
                lit(cod_ind).cast("int").alias("cod_indicador"),
                coalesce(col("nivel_sigilo_derived"), lit(0)).alias("id_sigilo"),
                col("cod_natureza"),
                current_timestamp().alias("dth_atualizacao"),
                col("cod_oj").cast("decimal(38,18)").alias("cod_ult_oj"),
                col("cod_relator").cast("decimal(38,18)").alias("cod_ult_magistrado"),
                lit(int(meta_ano)).cast("int").alias("meta_ano"),
            )
        )

    # ── Orchestrate ───────────────────────────────────────────────────────────
    def run(self, year: int, period_start: date, period_end: date) -> None:
        """
        Meta 01 ejud is an ANNUAL indicator.
        period_start is the CNJ cutoff start (e.g. 2025-12-21 or 2026-01-01).
        period_end is the reporting end date (e.g. 2026-12-31 or today's date).
        cnm01 = processes whose true first-ever distribution falls in [period_start, period_end].
        """
        meta_ano = str(year)
        self._relator_cutoff = period_end  # upper bound for cod_ult_magistrado / cod_ult_oj

        logger.info("=" * 70)
        logger.info(
            "ejud_metacnj_m01_emr  year=%d  period=%s → %s",
            year, period_start, period_end,
        )
        logger.info("=" * 70)

        # Step 1 — eligible coddoc
        eligible = self.build_eligible_coddoc()
        eligible.cache()
        n_elig = eligible.count()
        logger.info("eligible coddoc cached — count=%d", n_elig)

        # Step 2 — cnm01
        cnm01 = self.build_cnm01(period_start, period_end, eligible)
        cnm01.cache()
        n_cnm01 = cnm01.count()
        logger.info("cnm01 cached — count=%d", n_cnm01)

        # Step 3 — write cnm01
        _save_to_iceberg(self.spark, self._finalize(self._enrich(cnm01), "cnm01", meta_ano), self.COD_INDICADOR_MAP["cnm01"], self.IND_SISTEMA, year, "cnm01")

        # Step 4 — julgadom01 = union(primeirasentm01 + baixm01 + mpum01)
        # NOTE: julgadom01 uses the full eligible set — any eligible process judged
        # in the period counts, not only those that are also in cnm01.
        prim = self.build_primeirasentm01(period_start, period_end, eligible)
        prim.cache()
        n_prim = prim.count()
        logger.info("primeirasentm01 cached — count=%d", n_prim)

        baixm = self.build_baixm01(period_start, period_end, eligible)
        baixm.cache()
        n_baixm = baixm.count()
        logger.info("baixm01 cached — count=%d", n_baixm)

        mpum = self.build_mpum01(period_start, period_end, eligible)
        mpum.cache()
        n_mpum = mpum.count()
        logger.info("mpum01 cached — count=%d", n_mpum)

        # eligible kept alive — still needed for desm01 below

        julgadom01 = self.build_julgadom01(prim, baixm, mpum)
        julgadom01.cache()
        n_julgadom01 = julgadom01.count()
        logger.info("julgadom01 cached — count=%d  (prim=%d baixm=%d mpum=%d)",
                    n_julgadom01, n_prim, n_baixm, n_mpum)
        prim.unpersist()
        baixm.unpersist()
        mpum.unpersist()

        _save_to_iceberg(self.spark, self._finalize(self._enrich(julgadom01), "julgadom01", meta_ano), self.COD_INDICADOR_MAP["julgadom01"], self.IND_SISTEMA, year, "julgadom01")

        # Step 5 — desm01 FIRST (before susm01 — susm01 pool = cnm01 UNION desm01)
        # Pool: ALL eligible processes (Linux does not restrict to cnm01)
        desm01 = self.build_desm01(period_start, period_end, eligible, julgadom01)
        desm01.cache()
        n_desm = desm01.count()
        logger.info("desm01 cached — count=%d", n_desm)
        eligible.unpersist()  # done with eligible after desm01 consumes it
        _save_to_iceberg(self.spark, self._finalize(self._enrich(desm01), "desm01", meta_ano), self.COD_INDICADOR_MAP["desm01"], self.IND_SISTEMA, year, "desm01")

        # Step 6 — susm01: pool = cnm01 UNION desm01 (Linux pattern)
        susp_pool = cnm01.select("coddoc").unionAll(desm01.select("coddoc"))
        susm01 = self.build_susm01(susp_pool, period_end)
        n_susm = susm01.count()
        logger.info("susm01 count=%d", n_susm)
        _save_to_iceberg(self.spark, self._finalize(self._enrich(susm01), "susm01", meta_ano), self.COD_INDICADOR_MAP["susm01"], self.IND_SISTEMA, year, "susm01")

        desm01.unpersist()
        julgadom01.unpersist()
        cnm01.unpersist()

        logger.info(
            "Summary  cnm01=%d  julgadom01=%d  susm01=%d  desm01=%d",
            n_cnm01, n_julgadom01, n_susm, n_desm,
        )
        logger.info("=" * 70)
        logger.info("ejud_metacnj_m01_emr  DONE  year=%d  period=%s → %s", year, period_start, period_end)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    today = date.today()
    p = argparse.ArgumentParser(
        description="EJUD Meta 01 indicators → Iceberg temp_2inst_ouro.tb_2inst_metacnj"
    )
    p.add_argument(
        "--year", type=int, default=today.year,
        help="Reference year written to meta_ano partition (default: current year)",
    )
    p.add_argument(
        "--period-start", default=None,
        help="Start of the Meta 01 window YYYY-MM-DD "
             "(default: YEAR-01-01, e.g. 2026-01-01). "
             "Use the CNJ cutoff if different, e.g. 2025-12-21.",
    )
    p.add_argument(
        "--period-end", default=None,
        help="End of the Meta 01 window YYYY-MM-DD "
             "(default: YEAR-12-31, e.g. 2026-12-31 for year=2026).",
    )
    p.add_argument(
        "--config-path", default=_DEFAULT_CONFIG_PATH,
        help="S3 URI or local path to meta_config.json (default: S3 prod path)",
    )
    p.add_argument("--catalog",                  default=_CATALOG)
    p.add_argument("--db-situacao",              default=_DB_SITUACAO)
    p.add_argument("--db-bronze",                default=_DB_BRONZE)
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
    args = _parse_args(argv)
    cfg = _load_config(args.config_path)

    # Resolve target from config, allow CLI override for temp environments
    global _TARGET_DB, _TARGET_TABLE, _TARGET_LOC  # noqa: PLW0603
    cat_cfg = cfg["catalog"]
    _TARGET_DB    = args.target_db  or cat_cfg["target_db"]
    _TARGET_TABLE = cat_cfg["target_table"]
    _TARGET_LOC   = args.target_loc or cat_cfg["target_loc"]

    year = args.year
    period_start = (
        date.fromisoformat(args.period_start)
        if args.period_start
        else date(year, 1, 1)
    )
    period_end = (
        date.fromisoformat(args.period_end)
        if args.period_end
        else date(year, 12, 31)
    )

    spark = _create_spark_session(
        args.executor_memory,
        args.executor_memory_overhead,
        args.shuffle_partitions,
    )
    calc = MetaCNJM01EjudCalculator(
        spark=spark,
        cfg=cfg,
        catalog=args.catalog,
        db_situacao=args.db_situacao,
        db_bronze=args.db_bronze,
    )
    try:
        calc.run(year=year, period_start=period_start, period_end=period_end)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
