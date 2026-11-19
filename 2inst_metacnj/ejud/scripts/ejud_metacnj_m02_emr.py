#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_metacnj_m02_emr.py — EJUD Meta 02 — all indicators → Iceberg (ouro layer)

Fresh rewrite directly translated from production:
  C:\\Users\\felip\\Workspace\\linux\\meta_cnj2025

Single-script design: no external .py dependencies.
All data reads via spark.table() from the Glue catalog.

Target table : glue_catalog.temp_2inst_ouro.tb_2inst_metacnj
Partitions   : meta_ano, variavel

Variables calculated:
  dism02           Stock  — classe conhecimento, dist before cutoff, not judged
  dism_ant02       Stock  — same, dist <= 15 years before stock year
  primeirasentm02  Flow   — first sentence after cutoff (dism02 pool)
  baixm02          Flow   — first baixa/archiving after cutoff (dism02 pool)
  mpum02           Flow   — first MPU decision after cutoff (dism02 pool)
  julgadom02       Flow   — union of above three → first per process
  julgadom02_ant   Flow   — first judgment after cutoff (dism_ant02 pool)
  susm02           Snap   — first suspension after cutoff (dism02 pool, not judged)
  susm02_ant       Snap   — first suspension after cutoff (dism_ant02 pool, not judged)
  desm_ant02       Snap   — first levantamento after cutoff (dism_ant02 pool, not judged)

Key production dates (meta_config.json "meta_02"):
  period_start       = 2012-01-01
  period_end         = 2026-12-31   (use --period-end to override)
  meta02_cutoff_date = 2023-12-31   (pass via --cutoff-date)
  meta02_15year_cutoff = 2011-12-31 (auto-derived as date(year-15, 12, 31))

eligible coddoc:
  Computed inline:
    tipoprocesso(codextcnj IN CLASSE_CONHECIMENTO AND (codnat=1 OR NULL))  → cível
    UNION tipoprocesso(codextcnj IN CLASSE_OUTROS)                         → criminal
  + documento(indativo = 'S')   ← SEMI JOIN (active documents only)
  + competencia(codcompt NOT IN [9,12,21])
  → distinct codtipproc → INNER JOIN ejud.t_processo → distinct coddoc.

dism02 algorithm (sit-88 based — official CNJ rule):
  CNJ: "processos cuja data de início da primeira situação Pendente (88) ocorra
  antes do período, iniciada pelas situações: 9, 24, 61, 65, 81, sem julgamento."
  1. Scan sit-88 + trigger codes [9,24,61,65,81], DIST_START → cutoff_date.
  2. First sit-88 per coddoc (earliest dthrmov).
  3. Trigger validation: at least one trigger event with dthrmov <= first_88_dt.
  4. Eligible semi-join.
  5. Anti-join judgment (DIST_START → cutoff_date; sit-17 guard).
  6. Anti-join judmigracao.

dism_ant02 algorithm (same, two cutoffs):
  - First sit-88 bounded by meta15_cutoff (e.g. 2011-12-31).
  - Judgment anti-join extends to cutoff_date (e.g. 2023-12-31).
  Steps 1–6 same as dism02, meta15_cutoff replaces cutoff_date for step 1 only.

Usage:
    spark-submit --master yarn --deploy-mode client \
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
      s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts/ejud_metacnj_m02_emr.py \
      --year 2026 --cutoff-date 2023-12-31 2>&1 | \
      grep --line-buffered -E 'ejud_metacnj_m02|ERROR|Exception'
"""

import argparse
import json
import logging
import sys
from datetime import date, timedelta
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
logger = logging.getLogger("ejud_metacnj_m02_emr")


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
# Production target is fixed in ouro; CLI can override for temp/test environments.
_CATALOG        = "glue_catalog"
_WAREHOUSE      = "s3://sgdai-data-lake"
_TARGET_DB      = "dw_2inst_ouro"
_TARGET_TABLE   = "tb_ft_meta_analitico"
_TARGET_LOC     = f"{_WAREHOUSE}/ouro/{_TARGET_DB}/{_TARGET_TABLE}"
_PARTITION_COLS = ["meta_ano", "variavel"]

# ── Source tables ─────────────────────────────────────────────────────────────
_DB_SITUACAO    = "dw_2inst_ouro"
_SITUACAO_TABLE = "tb_tf_situacao_ejud"   # coddoc, situacao, dthrmov, dth_fim, cod_tip_mov, cod_classe, nivel_sigilo
# judmigracao exclusion is built INLINE from ejud.movimento — no prata pre-built table needed
# Rule: exclude coddoc if ANY movimento.usuincl LIKE 'USUARIO_%' OR = 'JUDMIGRACAO'



# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(shuffle_partitions: int = 400) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudMetaCNJ_M02")
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
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


# ── Iceberg DDL / write helpers ───────────────────────────────────────────────
def _ensure_target_table(spark: SparkSession) -> None:
    """
    Create tb_ft_meta_analitico if it does not yet exist.
    Uses the exact agreed DDL — does not infer schema from DataFrame.
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
    """Strip trailing .0 from numeric-ish string columns (e.g. '12.0' → '12')."""
    return regexp_replace(c.cast("string"), r"\.0$", "")


def _first_per_coddoc(df: DataFrame, order_col: str = "dthrmov") -> DataFrame:
    """Keep the first (oldest) row per coddoc ordered by order_col asc."""
    w = Window.partitionBy("coddoc").orderBy(col(order_col).asc())
    return (
        df.withColumn("_rn", row_number().over(w))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )


def _load_dism_from_table(spark: SparkSession, year: int, cod_ind: int) -> DataFrame:
    """
    Reload a dism partition written by a previous run from tb_ft_meta_analitico.

    Used by --skip-dism: avoids re-computing eligible + dism (the heavy step).
    Reconstructs the internal column layout expected by the flow-variable
    enrichment step in run():
      coddoc (BIGINT), dth_distribuicao (TIMESTAMP), cod_classe (STRING),
      cod_sigilo (INT), cod_oj (INT), cod_relator (INT),
      num_processo (STRING), cod_competencia (INT), cod_natureza (INT),
      id_assunto_principal (INT).
    """
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    return (
        spark.table(fqn)
        .filter((col("meta_ano") == str(year)) & (col("cod_indicador") == cod_ind))
        .select(
            col("id_processo").cast("bigint").alias("coddoc"),
            col("num_processo"),
            col("dth_evento_indicador").alias("dth_distribuicao"),
            col("cod_classe_cnj").cast("string").alias("cod_classe"),
            col("id_sigilo").alias("cod_sigilo"),
            col("cod_ult_oj").cast("int").alias("cod_oj"),
            col("cod_ult_magistrado").cast("int").alias("cod_relator"),
            col("cod_competencia"),
            col("cod_natureza"),
            col("id_assunto_principal"),
        )
    )


# ── Main calculator ───────────────────────────────────────────────────────────
class MetaCNJM02Calculator:
    """
    EJUD Meta 02 — all indicators.

    Production reference:
      stocks/meta_stocks.py  → dism02, dism_ant02
      variables/variables_calculator.py → all flow/snapshot variables
    """

    def __init__(
        self,
        spark: SparkSession,
        cfg: dict,
        catalog: str = _CATALOG,
        db_situacao: str = _DB_SITUACAO,
        db_prata: str = "dw_2inst_ouro",
        db_bronze: str = "ejud",
    ):
        self.spark       = spark
        self.cat         = catalog
        self.db_situacao = db_situacao
        self.db_prata    = db_prata
        self.db_bronze   = db_bronze

        cat_cfg = cfg["catalog"]
        sh      = cfg["shared"]
        sc      = sh["situacao_codes"]
        m2      = cfg["meta_02"]

        self.IND_SISTEMA       = cat_cfg["ind_sistema"]
        self.COD_INDICADOR_MAP = m2["cod_indicador_map"]

        self.DIST_START = date.fromisoformat(sh["dist_start"])

        # Trigger codes for dism sit-88 approach: [9,65,81,61,24]
        self.TRIGGER_CODES           = sc["distribution_codes_stock"]
        self.JUDGMENT_CODES          = sc["judgment"]
        self.PRIM_SENT_CODES         = sc["prim_sent"]
        self.BAIXA_CODES             = sc["baixa"]
        self.TRANSITION_BAIXA_CODES  = sc["transition_baixa"]
        self.MPU_CODES               = sc["mpu"]
        self.MPU_VALID_MOV_CODES     = sc["mpu_valid_mov"]
        self.SUSPENSION_CODES        = sc["suspension_add"]
        self.SUSPENSION_REMOVE_CODES = sc["suspension_remove"]
        self.DESM_COD_TIP_MOV        = sc["desm_cod_tip_mov"]

        self.CLASSE_CONHECIMENTO          = sh["classe_conhecimento"]
        self.CLASSE_CONHECIMENTO_CRIMINAL = sh["classe_conhecimento_criminal"]
        self.CLASSE_OUTROS                = sh["classe_outros"]
        self.COMPETENCIA_EXCLUDE          = sh["competencia_exclude"]

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _tbl(self, db: str, table: str) -> str:
        return f"{self.cat}.{db}.{table}"

    def _sit(
        self,
        codes,
        date_from: date,
        date_to: date,
        eligible_df: Optional[DataFrame] = None,
        include_dth_fim: bool = False,
    ) -> DataFrame:
        """
        Load situação events from tb_tf_situacao_ejud.

        Args:
            codes:        int or List[int] — situacao code(s) to filter.
            date_from:    dthrmov >= date_from.
            date_to:      dthrmov <= date_to.
            eligible_df:  Optional DataFrame with 'coddoc' column — semi-join filter.
            include_dth_fim: If True, include dth_fim column (for baixm transitions).

        Returns:
            DataFrame: coddoc, situacao, dthrmov, cod_classe, nivel_sigilo [, dth_fim, cod_tip_mov]
        """
        sit_tbl = self._tbl(self.db_situacao, _SITUACAO_TABLE)
        if isinstance(codes, int):
            codes = [codes]

        select_cols = [
            col("coddoc"),
            col("situacao"),
            col("dthrmov"),
            col("cod_classe"),
            col("nivel_sigilo"),
            col("cod_tip_mov"),
        ]
        if include_dth_fim:
            select_cols.append(col("dth_fim"))

        df = (
            self.spark.table(sit_tbl)
            .filter(col("situacao").isin(codes))
            .filter(col("dthrmov") >= lit(date_from))
            .filter(col("dthrmov") <= lit(date_to))
            .select(*select_cols)
        )

        if eligible_df is not None:
            df = df.join(
                eligible_df.select("coddoc").distinct(),
                on="coddoc",
                how="semi",
            )

        return df

    # ── 0. Eligible coddoc ────────────────────────────────────────────────────

    def build_eligible_coddoc(self) -> DataFrame:
        """
        Build the eligible coddoc set for Meta 02 (conhecimento + outros) inline.

        Meta 02 includes BOTH conhecimento (civil) and outros (criminal) processes,
        matching the production filtered_coddoc_classe_conhecimento parquet.

        Algorithm:
          Step 1a: tipoprocesso WHERE codextcnj IN CLASSE_CONHECIMENTO
                   AND (codnat=1 OR codnat IS NULL) → codtipproc
          Step 1b: tipoprocesso WHERE codextcnj IN CLASSE_OUTROS → codtipproc (any codnat)
          Step 1c: union both → eligible_codtipproc
          Step 2:  competencia WHERE codcompt NOT IN [1,9,12,21] → valid codcompt set
          Step 3:  documento WHERE indativo = 'S' → active coddoc set
          Step 4:  t_processo INNER JOIN codtipproc + codcompt; SEMI JOIN active_docs
                   → distinct coddoc

        Returns: DataFrame with single column 'coddoc' (distinct, LongType).
        """
        logger.info(
            "Building eligible coddoc (conhecimento + outros): "
            "tipoprocesso(civil codnat=1/NULL UNION criminal codnat=2, dropDuplicates codtipproc) "
            "+ competencia(codcompt NOT IN [9,12,21])"
        )

        tp = self.spark.table(self._tbl(self.db_bronze, "tipoprocesso"))

        # Step 1a: conhecimento — civil (codnat=1/NULL) + criminal (codnat=2 for criminal subset)
        elig_civ = (
            tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_CONHECIMENTO))
            .filter((_norm(col("codnat")).cast("int") == 1) | col("codnat").isNull())
            .select(col("codtipproc")).distinct()
        )
        elig_crim = (
            tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_CONHECIMENTO_CRIMINAL))
            .select(col("codtipproc")).distinct()
        )
        elig_conhecimento = elig_civ.union(elig_crim).distinct()

        # Step 1b: outros — no codnat restriction (codnat=2 valid)
        elig_outros = (
            tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_OUTROS))
            .select(col("codtipproc")).distinct()
        )

        # Step 1c: union
        eligible_codtipproc = elig_conhecimento.union(elig_outros).distinct()

        # Step 2: competencia — valid codcompt set (NOT IN competencia_exclude)
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
        eligible = (
            self.spark.table(self._tbl(self.db_bronze, "t_processo"))
            .select(
                "coddoc",
                col("codtipproc"),
                _norm(col("codcompt")).cast("int").alias("codcompt_int"),
            )
            .join(broadcast(eligible_codtipproc), on="codtipproc", how="inner")
            .join(broadcast(eligible_codcompt), on="codcompt_int", how="inner")
            .join(active_docs, on="coddoc", how="semi")
            .select("coddoc")
            .distinct()
        )

        logger.info("Eligible coddoc built (lazy)")
        return eligible

    # ── 1. dism02 stock ───────────────────────────────────────────────────────

    def build_dism02(self, cutoff_date: date, eligible: DataFrame) -> DataFrame:
        """
        dism02 — stock indicator (sit-88 based, official CNJ rule).

        CNJ: "processos cuja data de início da primeira situação Pendente (88) ocorra
        antes do período E não tenha ocorrido nenhuma situação indicada na variável
        julgadom2 até o período, desde que a situação 88 tenha sido iniciada pelas
        situações: 9, 24, 61, 65, 81."

        Algorithm:
          1. Scan sit-88 + trigger codes [9,24,61,65,81] from DIST_START to cutoff_date.
          2. First sit-88 per coddoc (earliest dthrmov).
          3. Trigger validation: at least one trigger event with dthrmov <= first_88_dt.
          4. Eligible semi-join.
          5. Anti-join judgment (sit-17 guard: only valid MPU cod_tip_mov counts).
          6. Anti-join judmigracao.

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo
        """
        logger.info("Building dism02 (sit-88 based): cutoff=%s", cutoff_date)

        # Trigger codes per CNJ rule: 9=Denúncia/queixa, 24=Distribuído,
        # 61=Recebido pelo Tribunal, 65=Fase processual iniciada,
        # 81=Classe evoluída para ação penal.
        trigger_codes = self.TRIGGER_CODES

        # Step 1: one scan — sit-88 + trigger events up to cutoff_date
        relevant = self._sit([88] + trigger_codes, self.DIST_START, cutoff_date)
        relevant.cache()

        # Step 2: first sit-88 per coddoc
        _w = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        first_88 = (
            relevant.filter(col("situacao") == 88)
            .withColumn("_rn", row_number().over(_w))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select("coddoc", col("dthrmov").alias("first_88_dt"), "cod_classe", "nivel_sigilo")
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
            .select("coddoc", "first_88_dt", "cod_classe", "nivel_sigilo")
            .dropDuplicates(["coddoc"])
        )
        relevant.unpersist()

        # Step 4: eligible semi-join
        valid_88 = valid_88.join(
            eligible.select("coddoc").distinct(), on="coddoc", how="semi"
        )

        # Step 5: anti-join judgment
        # sit-17 guard: sit-17 without valid MPU cod_tip_mov is NOT a real judgment.
        judged_coddoc = (
            self._sit(self.JUDGMENT_CODES, self.DIST_START, cutoff_date)
            .filter(
                (col("situacao").cast("int") != 17)
                | col("cod_tip_mov").cast("int").isin(self.MPU_VALID_MOV_CODES)
            )
            .select("coddoc").distinct()
        )

        # Step 6: anti-join judmigracao (lost/migrated processes)
        # Exclude rule: any coddoc with ANY movimento from JUDMIGRACAO or USUARIO_%
        judmig_coddoc = (
            self.spark.table(self._tbl(self.db_bronze, "movimento"))
            .filter(col("usuincl").like("USUARIO_%") | (col("usuincl") == "JUDMIGRACAO"))
            .select("coddoc").distinct()
        )

        result = (
            valid_88
            .join(judged_coddoc, on="coddoc", how="left_anti")
            .join(judmig_coddoc, on="coddoc", how="left_anti")
        )

        return result.select(
            col("coddoc"),
            col("first_88_dt").alias("dth_distribuicao"),
            col("cod_classe").cast("string"),
            col("nivel_sigilo").alias("cod_sigilo"),
        )

    # ── 2. dism_ant02 stock ───────────────────────────────────────────────────

    def build_dism_ant02(
        self,
        meta15_cutoff: date,
        cutoff_date:   date,
        eligible:      DataFrame,
    ) -> DataFrame:
        """
        dism02_ant — stock of processes 15+ years old (sit-88 based, official CNJ rule).

        IMPORTANT: two different cutoffs (unlike dism02 which uses the same date for both):
          - first sit-88 must be <= meta15_cutoff   (e.g. 2011-12-31) — distribution window
          - judgment anti-join extends to cutoff_date (e.g. 2023-12-31)
        A process distributed in 2005 stays in dism02_ant unless judged up to 2023-12-31.

        Algorithm:
          1. Scan sit-88 + trigger codes [9,24,61,65,81] from DIST_START to meta15_cutoff.
          2. First sit-88 per coddoc.
          3. Trigger validation.
          4. Eligible semi-join.
          5. Anti-join judgment (DIST_START → cutoff_date, NOT meta15_cutoff).
          6. Anti-join judmigracao.

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo
        """
        logger.info(
            "Building dism_ant02 (sit-88 based): dist_cutoff=%s  judg_cutoff=%s",
            meta15_cutoff, cutoff_date,
        )

        trigger_codes = self.TRIGGER_CODES

        # Step 1: sit-88 + trigger events up to meta15_cutoff (15-year boundary)
        relevant = self._sit([88] + trigger_codes, self.DIST_START, meta15_cutoff)
        relevant.cache()

        # Step 2: first sit-88 per coddoc
        _w_ant = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        first_88 = (
            relevant.filter(col("situacao") == 88)
            .withColumn("_rn", row_number().over(_w_ant))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select("coddoc", col("dthrmov").alias("first_88_dt"), "cod_classe", "nivel_sigilo")
        )

        # Step 3: trigger validation
        triggers = (
            relevant.filter(col("situacao").isin(trigger_codes))
            .select("coddoc", col("dthrmov").alias("trigger_dt"))
        )
        valid_88 = (
            first_88
            .join(triggers, on="coddoc", how="inner")
            .filter(col("trigger_dt") <= col("first_88_dt"))
            .select("coddoc", "first_88_dt", "cod_classe", "nivel_sigilo")
            .dropDuplicates(["coddoc"])
        )
        relevant.unpersist()

        # Step 4: eligible semi-join
        valid_88 = valid_88.join(
            eligible.select("coddoc").distinct(), on="coddoc", how="semi"
        )

        # Step 5: anti-join judgment — extends to cutoff_date (NOT meta15_cutoff)
        # sit-17 guard same as dism02.
        judged_coddoc = (
            self._sit(self.JUDGMENT_CODES, self.DIST_START, cutoff_date)
            .filter(
                (col("situacao").cast("int") != 17)
                | col("cod_tip_mov").cast("int").isin(self.MPU_VALID_MOV_CODES)
            )
            .select("coddoc").distinct()
        )

        # Step 6: anti-join judmigracao
        judmig_coddoc = (
            self.spark.table(self._tbl(self.db_bronze, "movimento"))
            .filter(col("usuincl").like("USUARIO_%") | (col("usuincl") == "JUDMIGRACAO"))
            .select("coddoc")
            .distinct()
        )

        result = (
            valid_88
            .join(judged_coddoc, on="coddoc", how="left_anti")
            .join(judmig_coddoc, on="coddoc", how="left_anti")
        )

        return result.select(
            col("coddoc"),
            col("first_88_dt").alias("dth_distribuicao"),
            col("cod_classe").cast("string"),
            col("nivel_sigilo").alias("cod_sigilo"),
        )

    # ── 3. primeirasentm02 ────────────────────────────────────────────────────

    def build_primeirasentm02(
        self, cutoff_date: date, period_end: date, eligible_df: DataFrame
    ) -> DataFrame:
        """
        primeirasentm02 — first sentence per process after cutoff.

        Production: _calculate_primeirasentm_with_eligible_processes()
        Situacoes: PRIM_SENT_CODES [27,28,62,18,29,90,129]
        Date range: cutoff_date → period_end
        Filter: eligible_df (dism02 pool)
        Dedup: FIRST per coddoc ordered by dthrmov asc

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building primeirasentm02: %s → %s", cutoff_date, period_end)

        events = self._sit(self.PRIM_SENT_CODES, cutoff_date, period_end, eligible_df=eligible_df)
        final  = _first_per_coddoc(events, "dthrmov")

        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 4. baixm02 ────────────────────────────────────────────────────────────

    def build_baixm02(
        self, cutoff_date: date, period_end: date, eligible_df: DataFrame,
        period_start_88: Optional[date] = None,
    ) -> DataFrame:
        """
        baixm02 — first baixa/archiving per process after cutoff.

        Production: _calculate_baixm_with_eligible_processes()

        Two event sources:
          A. Direct baixa: sit[BAIXA_CODES], cutoff_date → period_end
          B. Transition: sit-88 (from period_start_88 → period_end) whose dth_fim
             matches a TRANSITION_BAIXA_CODES event dthrmov in the same coddoc
             AND the transition event occurred in cutoff_date → period_end.

        Both unioned → FIRST per coddoc.

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building baixm02: %s → %s", cutoff_date, period_end)
        if period_start_88 is None:
            period_start_88 = date(2012, 1, 1)

        # A. Direct baixa events
        baixa_direct = self._sit(self.BAIXA_CODES, cutoff_date, period_end, eligible_df=eligible_df)

        # B. Transition baixas: sit-88 closed into a TRANSITION_BAIXA_CODE event
        try:
            sit88 = self._sit(
                88, period_start_88, period_end,
                eligible_df=eligible_df,
                include_dth_fim=True,
            )
            transitions = self._sit(
                self.TRANSITION_BAIXA_CODES, cutoff_date, period_end, eligible_df=eligible_df
            )

            # Match sit-88.dth_fim == transition.dthrmov (same coddoc)
            matched = (
                sit88.alias("s88")
                .join(
                    transitions.select(
                        col("coddoc").alias("_t_coddoc"),
                        col("dthrmov").alias("_t_dthrmov"),
                    ),
                    on=(
                        (col("s88.coddoc") == col("_t_coddoc")) &
                        (col("s88.dth_fim") == col("_t_dthrmov"))
                    ),
                    how="inner",
                )
                .select(
                    col("s88.coddoc"),
                    col("s88.situacao"),
                    col("s88.dthrmov"),
                    col("s88.cod_classe"),
                    col("s88.nivel_sigilo"),
                    col("s88.cod_tip_mov"),
                )
            )

            all_baixas = baixa_direct.unionByName(matched, allowMissingColumns=True)
            logger.info("baixm02: direct baixas + sit-88 transitions combined")
        except Exception as exc:
            logger.warning("baixm02: transition baixas failed (%s) — using direct baixas only", exc)
            all_baixas = baixa_direct

        final = _first_per_coddoc(all_baixas, "dthrmov")
        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 5. mpum02 ─────────────────────────────────────────────────────────────

    def build_mpum02(
        self, cutoff_date: date, period_end: date, eligible_df: DataFrame
    ) -> DataFrame:
        """
        mpum02 — first MPU/PPUM decision per process after cutoff.

        Production: _calculate_mpum_with_eligible_processes()
        Situacoes: MPU_CODES [17, 34] restricted to MPU_VALID_MOV_CODES.
        Date range: cutoff_date → period_end
        Filter: eligible_df

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building mpum02: %s → %s", cutoff_date, period_end)

        events = (
            self._sit(self.MPU_CODES, cutoff_date, period_end, eligible_df=eligible_df)
            .filter(col("cod_tip_mov").isin(self.MPU_VALID_MOV_CODES))
        )
        final = _first_per_coddoc(events, "dthrmov")
        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 6. julgadom02 ─────────────────────────────────────────────────────────

    def build_julgadom02(
        self,
        prim_df: DataFrame,
        baixm_df: DataFrame,
        mpum_df: DataFrame,
    ) -> DataFrame:
        """
        julgadom02 — first judgment event per process after cutoff.

        Production: _calculate_julgadom_with_eligible_processes()

        Algorithm:
          1. Union primeirasentm02 + baixm02 + mpum02 (all already have valid mov filter)
          2. Apply sit-17 cod_tip_mov guard (belt-and-suspenders)
          3. FIRST per coddoc ordered by dth_distribuicao asc

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building julgadom02: union(primeirasentm, baixm, mpum)")

        all_events = prim_df.unionByName(baixm_df, allowMissingColumns=True) \
                            .unionByName(mpum_df,  allowMissingColumns=True)

        # Belt-and-suspenders: situacao 17 MUST have valid cod_tip_mov
        # (mpum already filtered, but guard in case schema changes)
        all_events = all_events.filter(
            (col("situacao") != 17) |
            col("cod_tip_mov").isin(self.MPU_VALID_MOV_CODES)
        )

        return _first_per_coddoc(all_events, "dth_distribuicao")

    # ── 7. julgadom02_ant ─────────────────────────────────────────────────────

    def build_julgadom_ant02(
        self, cutoff_date: date, period_end: date, dism_ant_df: DataFrame
    ) -> DataFrame:
        """
        julgadom02_ant — first judgment event per dism_ant process after cutoff.

        Production: _calculate_julgadom_ant()
        Situacoes: JUDGMENT_CODES (full set, sit-17 requires valid cod_tip_mov)
        Date range: cutoff_date → period_end
        Filter: dism_ant02 eligible pool (dism_ant_df)

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building julgadom02_ant: %s → %s", cutoff_date, period_end)

        events = self._sit(self.JUDGMENT_CODES, cutoff_date, period_end, eligible_df=dism_ant_df)

        # sit-17 must have valid cod_tip_mov
        events = events.filter(
            (col("situacao") != 17) |
            col("cod_tip_mov").isin(self.MPU_VALID_MOV_CODES)
        )

        final = _first_per_coddoc(events, "dthrmov")
        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 8. susm02 ─────────────────────────────────────────────────────────────

    def build_susm02(
        self,
        cutoff_date: date,
        period_end: date,
        dism_df: DataFrame,
        julgadom_df: DataFrame,
    ) -> DataFrame:
        """
        susm02 — first suspension event per dism02 process after cutoff,
                 excluding already-judged processes.

        Production: _calculate_susm_with_eligible_processes()
        Situacoes: SUSPENSION_CODES (without 128)
        Date range: cutoff_date → period_end
        Filter: dism02 pool (dism_df), NOT in julgadom02

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building susm02: %s → %s", cutoff_date, period_end)

        events = self._sit(self.SUSPENSION_CODES, cutoff_date, period_end, eligible_df=dism_df)

        # Exclude judged processes
        judged_coddoc = julgadom_df.select("coddoc").distinct()
        events = events.join(judged_coddoc, on="coddoc", how="left_anti")

        final = _first_per_coddoc(events, "dthrmov")
        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 9. susm02_ant ─────────────────────────────────────────────────────────

    def build_susm_ant02(
        self,
        cutoff_date: date,
        period_end: date,
        dism_ant_df: DataFrame,
        julgadom_ant_df: DataFrame,
    ) -> DataFrame:
        """
        susm02_ant — first suspension event per dism_ant02 process after cutoff,
                     excluding already-judged processes.

        Production: _calculate_susm_ant()
        Situacoes: SUSPENSION_CODES
        Date range: cutoff_date → period_end
        Filter: dism_ant02 pool, NOT in julgadom02_ant

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building susm02_ant: %s → %s", cutoff_date, period_end)

        events = self._sit(self.SUSPENSION_CODES, cutoff_date, period_end, eligible_df=dism_ant_df)

        judged_coddoc = julgadom_ant_df.select("coddoc").distinct()
        events = events.join(judged_coddoc, on="coddoc", how="left_anti")

        final = _first_per_coddoc(events, "dthrmov")
        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 10. desm_ant02 ────────────────────────────────────────────────────────

    def build_desm_ant02(
        self,
        cutoff_date: date,
        period_end: date,
        dism_ant_df: DataFrame,
        julgadom_ant_df: DataFrame,
    ) -> DataFrame:
        """
        desm02_ant — first levantamento de suspensão event per dism02_ant process
                     after cutoff, excluding already-judged processes.

        Situacao-based approach: filter situacao IN SUSPENSION_REMOVE_CODES (20, 97–107, 138).
        The levantamento events in tb_tf_situacao_ejud are stored under their original
        EJUD situacao codes — NOT as cod_tip_mov MNI values (those are unpopulated).

        Date range: cutoff_date → period_end
        Pool: dism02_ant, NOT in julgadom02_ant
        Dedup: first per coddoc (earliest dthrmov asc)

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, cod_sigilo, situacao, cod_tip_mov
        """
        logger.info("Building desm02_ant via situacao %s: %s → %s",
                    self.SUSPENSION_REMOVE_CODES, cutoff_date, period_end)

        sit_tbl = self._tbl(self.db_situacao, _SITUACAO_TABLE)
        events = (
            self.spark.table(sit_tbl)
            .filter(col("situacao").isin(self.SUSPENSION_REMOVE_CODES))
            .filter(col("dthrmov") >= lit(cutoff_date))
            .filter(col("dthrmov") <= lit(period_end))
            .select(
                col("coddoc"), col("situacao"), col("dthrmov"),
                col("cod_classe"), col("nivel_sigilo"), col("cod_tip_mov"),
            )
            .join(dism_ant_df.select("coddoc").distinct(), on="coddoc", how="semi")
        )

        judged_coddoc = julgadom_ant_df.select("coddoc").distinct()
        events = events.join(judged_coddoc, on="coddoc", how="left_anti")

        final = _first_per_coddoc(events, "dthrmov")
        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── Enrichment ────────────────────────────────────────────────────────────

    def _enrich_t_processo(self, df: DataFrame) -> DataFrame:
        """
        Left-join ejud.t_processo to add:
          num_processo         — 20-char CNJ format
          cod_competencia      — codcompt
          cod_natureza         — codnat from t_processo if present, otherwise from tipoprocesso
          nivel_sigilo_derived — DECODE(IndSuperSigil,'S',5, DECODE(IndSigil,'S',4,
                                   DECODE(IndSegrJust,'S',1,0)))
                                 always used as id_sigilo (situacao nivel_sigilo is ignored)

        codnat lives in tipoprocesso in this cluster (not in t_processo).
        When t_processo.codnat is absent, we join tipoprocesso via codtipproc.
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
            # codnat available directly in t_processo
            tp = tp_raw.select(
                col("coddoc"),
                num_proc_expr.alias("num_processo"),
                _norm(col("codcompt")).cast("int").alias("cod_competencia"),
                _norm(col("codnat")).cast("int").alias("cod_natureza"),
                sigilo_expr,
            )
        else:
            # codnat not in t_processo — resolve via tipoprocesso.codnat
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
        """Apply all enrichment steps (t_processo + assunto + relator)."""
        df = self._enrich_t_processo(df)
        df = self._enrich_assunto(df)
        df = self._enrich_relator(df)
        return df

    # ── Finalize output schema ────────────────────────────────────────────────

    def _finalize(self, df: DataFrame, variavel: str, meta_ano: str) -> DataFrame:
        """
        Map enriched DataFrame to the target schema of tb_2inst_metacnj.

        Required input columns: coddoc, dth_distribuicao, cod_classe, cod_sigilo.
        Optional (added as NULL if missing): num_processo, cod_competencia,
          cod_natureza, id_assunto_principal, cod_relator, cod_oj.
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
                col("cod_classe").cast("int").alias("cod_classe_cnj"),
                col("id_assunto_principal"),
                col("cod_competencia"),
                col("dth_distribuicao").alias("dth_evento_indicador"),
                lit(cod_ind).cast("int").alias("cod_indicador"),
                coalesce(col("nivel_sigilo_derived"), lit(0)).alias("id_sigilo"),
                col("cod_natureza"),
                current_timestamp().alias("dth_atualizacao"),
                col("cod_oj").cast("decimal(38,18)").alias("cod_ult_oj"),
                col("cod_relator").cast("decimal(38,18)").alias("cod_ult_magistrado"),
                lit(int(meta_ano)).cast("int").alias("meta_ano"),
            )
        )

    # ── Orchestration ─────────────────────────────────────────────────────────

    def run(
        self,
        year: int,
        cutoff_date: date,
        period_end: Optional[date] = None,
        skip_dism: bool = False,
    ) -> None:
        """
        Calculate and write all Meta 02 indicators to Iceberg.

        Args:
            year:         Stock year used as meta_ano and to derive meta15_cutoff.
            cutoff_date:  CNJ distribution cutoff (e.g. 2023-12-31 for meta02/2024).
                          dism02 = processes distributed AND not judged BEFORE this date.
            period_end:   End of the flow/snapshot window (default: date(year, 12, 31)).
                          Production uses 2026-12-31 for meta_ano=2026.
            skip_dism:    If True, skip the expensive eligible+dism build steps and reload
                          dism02 / dism02_ant from the already-written Iceberg partitions.
                          Use this on reruns when the stock (dism) is known to be stable.
        """
        if period_end is None:
            period_end = date(year, 12, 31)

        meta_ano      = str(year)
        meta15_cutoff = date(year - 15, 12, 31)
        self._relator_cutoff = period_end  # upper bound for cod_ult_magistrado / cod_ult_oj      # e.g. 2011-12-31 for year=2026
        # flow_start: events from the day AFTER the cutoff.
        # cutoff_date (e.g. 2023-12-31) is the exclusive boundary — stock counts
        # distributions/judgments UP TO AND INCLUDING that date; flow variables
        # count events STRICTLY AFTER it (2024-01-01 onwards).
        flow_start    = cutoff_date + timedelta(days=1)

        logger.info("=" * 70)
        logger.info(
            "ejud_metacnj_m02_emr  year=%d  cutoff=%s  flow_start=%s  meta15=%s  "
            "period_end=%s  skip_dism=%s",
            year, cutoff_date, flow_start, meta15_cutoff, period_end, skip_dism,
        )
        logger.info("=" * 70)

        if not skip_dism:
            # ─ Step 0: eligible coddoc (shared across all variables) ──────────
            eligible = self.build_eligible_coddoc()
            eligible.cache()
            n_elig = eligible.count()
            logger.info("Eligible coddoc cached — count=%d", n_elig)

            # ─ Step 1: dism02 stock ───────────────────────────────────────────
            dism02 = self.build_dism02(cutoff_date, eligible)
            dism02.cache()
            n_dism02 = dism02.count()
            logger.info("dism02 cached — count=%d", n_dism02)

            # ─ Step 2: dism_ant02 stock ───────────────────────────────────────
            dism_ant02 = self.build_dism_ant02(meta15_cutoff, cutoff_date, eligible)
            dism_ant02.cache()
            n_dism_ant02 = dism_ant02.count()
            logger.info("dism_ant02 cached — count=%d", n_dism_ant02)
        else:
            logger.info(
                "--skip-dism: reloading dism02 + dism02_ant from Iceberg "
                "(year=%d, table=%s.%s.%s)…",
                year, _CATALOG, _TARGET_DB, _TARGET_TABLE,
            )
            n_elig = n_dism02 = n_dism_ant02 = -1   # unknown (not recomputed)
            dism02     = _load_dism_from_table(self.spark, year, self.COD_INDICADOR_MAP["dism02"])
            dism_ant02 = _load_dism_from_table(self.spark, year, self.COD_INDICADOR_MAP["dism02_ant"])
            dism02.cache()
            dism_ant02.cache()
            n_dism02     = dism02.count()
            n_dism_ant02 = dism_ant02.count()
            logger.info("dism02 reloaded — count=%d", n_dism02)
            logger.info("dism_ant02 reloaded — count=%d", n_dism_ant02)

        dism02_coddoc     = dism02.select("coddoc").distinct()
        dism_ant02_coddoc = dism_ant02.select("coddoc").distinct()

        # ─ Step 3: flow variables — dism02 pool (start AFTER cutoff_date) ────
        prim   = self.build_primeirasentm02(flow_start, period_end, dism02_coddoc)
        prim.cache()
        n_prim = prim.count()
        logger.info("primeirasentm02 cached — count=%d", n_prim)

        baixm  = self.build_baixm02(flow_start, period_end, dism02_coddoc)
        baixm.cache()
        n_baixm = baixm.count()
        logger.info("baixm02 cached — count=%d", n_baixm)

        mpum   = self.build_mpum02(flow_start, period_end, dism02_coddoc)
        mpum.cache()
        n_mpum = mpum.count()
        logger.info("mpum02 cached — count=%d", n_mpum)

        julgadom = self.build_julgadom02(prim, baixm, mpum)
        julgadom.cache()
        n_julgadom = julgadom.count()
        logger.info("julgadom02 cached — count=%d", n_julgadom)
        julgadom_coddoc = julgadom.select("coddoc").distinct()

        # ─ Step 4: flow variables — dism_ant02 pool (start AFTER cutoff_date) ─
        julgadom_ant = self.build_julgadom_ant02(flow_start, period_end, dism_ant02_coddoc)
        julgadom_ant.cache()
        n_julgadom_ant = julgadom_ant.count()
        logger.info("julgadom02_ant cached — count=%d", n_julgadom_ant)
        julgadom_ant_coddoc = julgadom_ant.select("coddoc").distinct()

        # ─ Step 5: snapshot variables — dism02 pool (start AFTER cutoff_date) ─
        susm02 = self.build_susm02(flow_start, period_end, dism02_coddoc, julgadom_coddoc)
        susm02.cache()
        n_susm02 = susm02.count()
        logger.info("susm02 cached — count=%d", n_susm02)

        # ─ Step 6: snapshot variables — dism_ant02 pool ──────────────────────
        susm_ant02 = self.build_susm_ant02(flow_start, period_end, dism_ant02_coddoc, julgadom_ant_coddoc)
        susm_ant02.cache()
        n_susm_ant = susm_ant02.count()
        logger.info("susm02_ant cached — count=%d", n_susm_ant)

        desm_ant02 = self.build_desm_ant02(flow_start, period_end, dism_ant02_coddoc, julgadom_ant_coddoc)
        desm_ant02.cache()
        n_desm_ant = desm_ant02.count()
        logger.info("desm_ant02 cached — count=%d", n_desm_ant)

        # ─ Step 7: enrich dism02 + dism_ant02 once (expensive joins) ─────────
        # When --skip-dism is used the data loaded from the table is already fully
        # enriched (num_processo, cod_oj, cod_relator, etc. were stored at write time).
        # In that case we skip the _enrich() calls and use the loaded frames directly.
        if not skip_dism:
            logger.info("Enriching dism02 base (t_processo + assunto + relator)…")
            dism02_enriched = self._enrich(dism02)
            dism02_enriched.cache()
            dism02.unpersist()

            logger.info("Enriching dism_ant02 base…")
            dism_ant02_enriched = self._enrich(dism_ant02)
            dism_ant02_enriched.cache()
            dism_ant02.unpersist()
        else:
            # dism02 / dism_ant02 were loaded via _load_dism_from_table which
            # already has the enrichment columns — use them directly.
            dism02_enriched     = dism02
            dism_ant02_enriched = dism_ant02

        # ─ Step 8: write all variables to Iceberg ────────────────────────────
        # primeirasentm02, baixm02, mpum02 are sub-components only — they have no
        # independent cod_indicador in the CNJ spec and are NOT written to the table.
        variables_to_write = [
            (dism02_enriched,     "dism02"),
            (dism_ant02_enriched, "dism02_ant"),   # prod name: dism02_ant
            (julgadom,            "julgadom02"),
            (julgadom_ant,        "julgadom02_ant"),
            (susm02,              "susm02"),
            (susm_ant02,          "susm02_ant"),
            (desm_ant02,          "desm02_ant"),   # prod name: desm02_ant
        ]

        _DISM_VARS = {"dism02", "dism02_ant"}
        _ANT_VARS  = {"dism02_ant", "julgadom02_ant", "susm02_ant", "desm02_ant"}

        for df, var in variables_to_write:
            if skip_dism and var in _DISM_VARS:
                logger.info("--skip-dism: skipping write for %s (already in table)", var)
                continue
            logger.info("Writing %s…", var)
            # Flow/snapshot variables use dism enriched attributes for enrichment.
            # _ant variables draw from dism_ant02_enriched; others from dism02_enriched.
            if var not in _DISM_VARS:
                base_enrich = dism_ant02_enriched if var in _ANT_VARS else dism02_enriched
                enrich_cols = [c for c in base_enrich.columns
                               if c in ("num_processo", "cod_competencia", "cod_natureza",
                                        "id_assunto_principal", "cod_relator", "cod_oj",
                                        "cod_sigilo")]  # cod_sigilo aliased in dism build
                base_join = base_enrich.select("coddoc", *enrich_cols).distinct()
                df = df.join(base_join, on="coddoc", how="left")

            out = self._finalize(df, var, meta_ano)
            _save_to_iceberg(self.spark, out, cod_ind=self.COD_INDICADOR_MAP[var], sistema=self.IND_SISTEMA, meta_ano_int=year, variavel=var)

        # ─ Cleanup ───────────────────────────────────────────────────────────
        to_unpersist = [dism02_enriched, dism_ant02_enriched, prim, baixm, mpum,
                        julgadom, julgadom_ant, susm02, susm_ant02, desm_ant02]
        if not skip_dism:
            to_unpersist.append(eligible)   # eligible is not created when --skip-dism
        for cached in to_unpersist:
            try:
                cached.unpersist()
            except Exception:
                pass

        logger.info("=" * 70)
        logger.info(
            "SUMMARY  dism02=%d  dism_ant02=%d  prim=%d  baixm=%d  mpum=%d  "
            "julgadom=%d  julgadom_ant=%d  susm=%d  susm_ant=%d  desm_ant=%d  "
            "skip_dism=%s",
            n_dism02, n_dism_ant02, n_prim, n_baixm, n_mpum,
            n_julgadom, n_julgadom_ant, n_susm02, n_susm_ant, n_desm_ant,
            skip_dism,
        )
        logger.info(
            "ejud_metacnj_m02_emr  DONE  year=%d  cutoff=%s", year, cutoff_date
        )
        logger.info("=" * 70)


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args(argv=None):
    today = date.today()
    p = argparse.ArgumentParser(
        description="EJUD Meta 02 — all indicators → Iceberg dw_2inst_ouro.tb_ft_meta_analitico"
    )
    p.add_argument(
        "--year", type=int, default=today.year,
        help="Stock year / meta_ano (default: current year)",
    )
    p.add_argument(
        "--cutoff-date", required=True,
        help="CNJ distribution cutoff date YYYY-MM-DD (e.g. 2023-12-31). "
             "dism02 = distributed AND not judged BEFORE this date.",
    )
    p.add_argument(
        "--period-end", default=None,
        help="End of the flow/snapshot window YYYY-MM-DD "
             "(default: date(year, 12, 31), e.g. 2026-12-31 for year=2026).",
    )
    p.add_argument(
        "--shuffle-partitions", type=int, default=400,
        help="spark.sql.shuffle.partitions (default: 400)",
    )
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
    p.add_argument(
        "--skip-dism", action="store_true", default=False,
        help="Skip the eligible+dism build steps and reload dism02/dism02_ant from the "
             "already-written Iceberg partitions. Use on reruns when the stock is stable "
             "(dism is historical and won't change for past years).",
    )
    p.add_argument(
        "--config-path",
        default=_DEFAULT_CONFIG_PATH,
        help="Path to meta_config.json (local or s3://...)",
    )
    p.add_argument("--catalog",     default=_CATALOG)
    p.add_argument("--db-situacao", default=_DB_SITUACAO)
    p.add_argument("--db-bronze",   default="ejud")
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    cfg = _load_config(args.config_path)

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

    cutoff_date = date.fromisoformat(args.cutoff_date)
    period_end  = date.fromisoformat(args.period_end) if args.period_end else None

    spark = _create_spark_session(shuffle_partitions=args.shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")

    try:
        calc = MetaCNJM02Calculator(
            spark=spark,
            cfg=cfg,
            catalog=args.catalog,
            db_situacao=args.db_situacao,
            db_bronze=args.db_bronze,
        )
        calc.run(
            year=args.year,
            cutoff_date=cutoff_date,
            period_end=period_end,
            skip_dism=args.skip_dism,
        )
    except Exception:
        logger.exception("Fatal error in ejud_metacnj_m02_emr")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
