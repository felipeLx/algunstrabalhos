#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_metacnj_mNN_emr.py — EJUD Assunto-based Metas (04, 06, 07, 08, 10, 11)
                           Parametric script — select meta with --meta 04|06|07|08|10|11

Single-script design: no external .py dependencies.
All data reads via spark.table() from the Glue catalog.

Source   : glue_catalog.dw_2inst_ouro.tb_tf_situacao_ejud
           glue_catalog.ejud.tipoprocesso
           glue_catalog.ejud.t_processo
           glue_catalog.ejud.processoassunto   ← assunto eligibility (any numord); priority-ranked for id_assunto_principal
           glue_catalog.ejud.movimentorelator
           glue_catalog.ejud.movimento             ← judmigracao exclusion (inline)

Target   : glue_catalog.dw_2inst_ouro.tb_ft_meta_analitico
           s3://sgdai-data-lake/ouro/dw_2inst_ouro/tb_ft_meta_analitico
           Partitioned by (meta_ano, variavel)

Eligibility (all assunto metas):
    tipoprocesso(codextcnj IN CLASSE_CONHECIMENTO AND codnat = 1)
    → distinct codtipproc
    + competencia(codcompt NOT IN [9,12,21])   ← INNER JOIN via t_processo.codcompt
    + documento(indativo = 'S')                   ← SEMI JOIN (active documents only)
    → INNER JOIN t_processo on codtipproc + codcompt; SEMI JOIN active_docs + processoassunto
    → distinct coddoc

Variables written per run (no _ant branch for these metas):
    dism{NN}     — stock: first sit-88 before cutoff, triggered by [9,24,61,65,81], not judged
    julgadom{NN} — union(primeirasentm, baixm, mpum) first per coddoc in flow window
    susm{NN}     — suspended from dism pool at period_end

Date conventions:
    cutoff_date  — processes distributed AND not judged BEFORE this date = stock
    flow_start   = cutoff_date + 1 day — flow variables START from this date
    period_end   — flow variables end here (default: date(year, 12, 31))

Cutoff defaults per meta (for year=2026, source: meta_cnj2025/config/meta_config.json):
    04 → date(year-4, 12, 31) = 2022-12-31
    06 → date(year-1, 12, 31) = 2025-12-31
    07 → date(year-1, 12, 31) = 2025-12-31
    08 → date(year-2, 12, 31) = 2024-12-31
    10 → date(year-2, 12, 31) = 2024-12-31
    11 → date(year-1, 12, 31) = 2025-12-31

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts/ejud_metacnj_mNN_emr.py \\
      --meta 10 --year 2026  2>&1 | \\
      grep --line-buffered -E 'ejud_metacnj_mNN|ERROR|Exception'

    # override cutoff when needed:
    spark-submit ... ejud_metacnj_mNN_emr.py --meta 08 --year 2026 --cutoff-date 2024-12-31

Validate in Athena:
    SELECT variavel, COUNT(*) AS n
    FROM dw_2inst_ouro.tb_ft_meta_analitico
    WHERE meta_ano = '2026' AND cd_meta = '10' AND cod_sistema = 1
    GROUP BY variavel ORDER BY variavel;
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import boto3

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    broadcast, coalesce, col, current_timestamp, lit, lpad, regexp_replace, row_number,
    concat_ws, upper, when,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_metacnj_mNN_emr")


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

# ── Source tables ─────────────────────────────────────────────────────────────
_DB_SITUACAO    = "dw_2inst_ouro"
_SITUACAO_TABLE = "tb_tf_situacao_ejud"
_DB_BRONZE      = "ejud"
_DB_PRATA       = "dw_2inst_ouro"


# ── Meta registry ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MetaConfig:
    meta_number:      str            # "04", "06", etc.
    assunto_codes:    List[int]      # processoassunto.codass IN this list
    cutoff_offset:    int            # default cutoff = date(year - offset, 12, 31)
    cod_indicadors:   Dict[str, int] # {"dism04": 13, "julgadom04": 14, "susm04": 15}
    # Priority ordering for id_assunto_principal when a process has subjects from
    # multiple sub-groups within the same meta.
    # Tuple of sub-group code-sets, index 0 = highest priority.
    # A codass from sub-group[0] always wins over sub-group[1] regardless of numord.
    # Ties within the same sub-group broken by numord ASC (lowest numord wins).
    # Empty tuple = single sub-group: just pick lowest numord from assunto_codes.
    assunto_priority: Tuple[Tuple[int, ...], ...] = ()

    def v_dism(self)     -> str: return f"dism{self.meta_number}"
    def v_julgadom(self) -> str: return f"julgadom{self.meta_number}"
    def v_susm(self)     -> str: return f"susm{self.meta_number}"
    def v_desm(self)     -> str: return f"desm{self.meta_number}"
    def cd_meta(self)    -> str: return self.meta_number


_VALID_METAS = ["04", "06", "07", "08", "10", "11"]


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudMetaCNJ_mNN")
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


# ── Iceberg table helpers ─────────────────────────────────────────────────────
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


def _first_per_coddoc(df: DataFrame, order_col: str = "dthrmov") -> DataFrame:
    """Keep the earliest row per coddoc ordered by order_col asc."""
    w = Window.partitionBy("coddoc").orderBy(col(order_col).asc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def _load_dism_from_table(spark: SparkSession, year: int, variavel: str, cod_indicador: int) -> DataFrame:
    """
    Reload a dism partition written by a previous run from tb_ft_meta_analitico.

    Used by --skip-dism: avoids re-computing eligible + dism (the heavy step).
    Reconstructs the internal column layout expected by _enrich-based enrichment
    in run() — matching the column names produced by _enrich_relator (mNN variant):
      coddoc (BIGINT), dth_distribuicao (TIMESTAMP), cod_classe (STRING),
      cod_sigilo (INT), cod_ult_oj (INT), cod_ult_magistrado (INT),
      num_processo (STRING), cod_competencia (INT), cod_natureza (INT),
      id_assunto_principal (INT).

    Note: column names here differ from m02 — mNN uses cod_ult_oj / cod_ult_magistrado
    (not the cod_oj / cod_relator aliases used by m02's _enrich_relator).
    """
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    return (
        spark.table(fqn)
        .filter((col("meta_ano") == str(year)) & (col("cod_indicador") == cod_indicador))
        .select(
            col("id_processo").cast("bigint").alias("coddoc"),
            col("num_processo"),
            col("dth_evento_indicador").alias("dth_distribuicao"),
            col("cod_classe_cnj").cast("string").alias("cod_classe"),
            col("id_sigilo").alias("cod_sigilo"),
            col("cod_ult_oj").cast("int"),
            col("cod_ult_magistrado").cast("int"),
            col("cod_competencia"),
            col("cod_natureza"),
            col("id_assunto_principal"),
        )
    )


# ── Main calculator ───────────────────────────────────────────────────────────
class MetaCNJAssuntoCalculator:
    """
    EJUD assunto-based metas — parametric calculator for metas 04, 06, 07, 08, 10, 11.

    All metas in this family share the same pipeline:
      dism{NN}     — stock (distributed before cutoff, pending sit-88 gate)
      julgadom{NN} — first judgment per process after cutoff
      susm{NN}     — suspended from dism pool at period_end

    The only per-meta differences are:
      - assunto_codes      (processoassunto.codass filter)
      - cutoff_date        (age threshold for stock)
      - cod_indicadors     (CNJ cod_indicador per variable)
    """

    def __init__(
        self,
        spark:       SparkSession,
        cfg:         dict,
        meta_num:    int,
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
        m_key   = f"meta_{meta_num:02d}"
        m_cfg   = cfg[m_key]

        self.IND_SISTEMA   = cat_cfg["ind_sistema"]
        self.DIST_START    = date.fromisoformat(sh["dist_start"])
        self.PEND_START    = date.fromisoformat(sh["pend_start"])
        self.COMPETENCIA_EXCLUDE = sh["competencia_exclude"]

        # Trigger codes for dism sit-88 approach
        self.TRIGGER_CODES           = sc["distribution_codes_stock"]
        self.JUDGMENT_CODES          = sc["judgment"]
        self.PRIM_SENT_CODES         = sc["prim_sent"]
        self.BAIXA_CODES             = sc["baixa"]
        self.MPU_CODES               = sc["mpu"]
        self.MPU_VALID_MOV_CODES     = sc["mpu_valid_mov"]
        self.SUSPENSION_CODES        = sc["suspension_add"]
        self.SUSPENSION_REMOVE_CODES = sc["suspension_remove"]
        self.DESM_COD_TIP_MOV        = sc["desm_cod_tip_mov"]

        self.CLASSE_CONHECIMENTO          = sh["classe_conhecimento"]
        self.CLASSE_CONHECIMENTO_CRIMINAL = sh["classe_conhecimento_criminal"]

        # Per-meta config
        self.COD_INDICADOR_MAP = m_cfg["cod_indicador_map"]
        self.CUTOFF_OFFSET     = m_cfg["cutoff_offset"]
        assunto_codes_raw      = m_cfg["assunto_codes"]
        priority_names         = m_cfg.get("assunto_priority", [])
        self.ASSUNTO_CODES     = [code for codes in assunto_codes_raw.values() for code in codes]
        self.ASSUNTO_PRIORITY  = tuple(
            tuple(assunto_codes_raw[name]) for name in priority_names
        )

        # Build MetaConfig for use in run()
        meta_num_str = f"{meta_num:02d}"
        self._meta_cfg = MetaConfig(
            meta_number      = meta_num_str,
            assunto_codes    = self.ASSUNTO_CODES,
            cutoff_offset    = self.CUTOFF_OFFSET,
            cod_indicadors   = {k: v for k, v in self.COD_INDICADOR_MAP.items()},
            assunto_priority = self.ASSUNTO_PRIORITY,
        )

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
        Returns: coddoc, situacao, dthrmov, cod_classe, nivel_sigilo, cod_tip_mov
        """
        sit_tbl = self._tbl(self.db_situacao, _SITUACAO_TABLE)
        if isinstance(codes, int):
            codes = [codes]

        select_cols = [
            col("coddoc"), col("situacao"), col("dthrmov"),
            col("cod_classe"), col("nivel_sigilo"), col("cod_tip_mov"),
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
            df = df.join(eligible_df.select("coddoc").distinct(), on="coddoc", how="semi")
        return df

    # ── 0. Eligible coddoc ────────────────────────────────────────────────────

    def build_eligible_coddoc(
        self,
        assunto_codes:    List[int],
        assunto_priority: Tuple[Tuple[int, ...], ...] = (),
    ) -> DataFrame:
        """
        Eligible coddoc = classe conhecimento ∩ assunto ∩ valid competencia.

        Assunto-based metas (04, 06-11) are always civil/cível — CLASSE_CONHECIMENTO only.
        CLASSE_OUTROS is not used here (criminal processes have no assunto codes for these metas).

        Step 1: tipoprocesso civil (codnat=1/NULL) UNION criminal subset (codnat=2) → distinct codtipproc
        Step 2: competencia WHERE codcompt NOT IN [1,9,12,21] → valid codcompt set
        Step 3: documento WHERE indativo = 'S' → active coddoc set
        Step 4: processoassunto WHERE codass IN assunto_codes → (coddoc, numord, codass_int)
                stored as self._assunto_raw for reuse in _enrich_assunto (no second scan).
        Step 5: t_processo INNER JOIN codtipproc + codcompt; SEMI JOIN active_docs + processoassunto
        → distinct coddoc
        """
        logger.info(
            "Building eligible coddoc: civil(codnat=1/NULL) + criminal(codnat=2) UNION, "
            "classe ∩ assunto (%d codes, %d priority groups)",
            len(assunto_codes), len(assunto_priority),
        )

        # Step 1: tipoprocesso — civil (codnat=1/NULL) + criminal conhecimento (codnat=2).
        # codnat=1/NULL → civil/cível variants of CLASSE_CONHECIMENTO codes
        # codnat=2      → criminal variants (CLASSE_CONHECIMENTO_CRIMINAL subset only)
        _tp = self.spark.table(self._tbl(self.db_bronze, "tipoprocesso"))
        _elig_civil = (
            _tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_CONHECIMENTO))
            .filter((_norm(col("codnat")).cast("int") == 1) | col("codnat").isNull())
            .select("codtipproc").distinct()
        )
        _elig_criminal = (
            _tp.filter(_norm(col("codextcnj")).cast("int").isin(self.CLASSE_CONHECIMENTO_CRIMINAL))
            .select("codtipproc").distinct()
        )
        eligible_codtipproc = _elig_civil.union(_elig_criminal).distinct()

        # Step 2: competencia — valid codcompt set
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

        # Step 4: processoassunto — read with numord for both eligibility and enrichment.
        # Eligibility: ANY codass from the meta's list qualifies (no numord restriction).
        # The full (coddoc, numord, codass_int) rows are stored as self._assunto_raw so
        # _enrich_assunto can reuse them with priority ranking — avoids re-scanning.
        assunto_raw = (
            self.spark.table(self._tbl(self.db_bronze, "processoassunto"))
            .filter(_norm(col("codass")).cast("int").isin(assunto_codes))
            .select(
                "coddoc",
                col("numord").cast("int").alias("numord"),
                _norm(col("codass")).cast("int").alias("codass_int"),
            )
        )
        # Store for _enrich_assunto — priority-aware id_assunto_principal resolution.
        self._assunto_raw      = assunto_raw          # type: ignore[attr-defined]
        self._assunto_priority = assunto_priority     # type: ignore[attr-defined]

        assunto_filter = assunto_raw.select("coddoc").distinct()

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
            .join(assunto_filter, on="coddoc", how="semi")
            .select("coddoc").distinct()
        )

    # ── 1. dism stock ─────────────────────────────────────────────────────────

    def build_dism(self, cutoff_date: date, eligible: DataFrame) -> DataFrame:
        """
        dism{NN} — stock indicator (sit-88 based, official CNJ rule).

        CNJ: "processos cuja data de início da primeira situação Pendente (88) ocorra
        antes do período, iniciada pelas situações: 9, 24, 61, 65, 81, sem julgamento."

        Algorithm:
          1. Scan sit-88 + trigger codes [9,24,61,65,81], DIST_START → cutoff_date.
          2. First sit-88 per coddoc (earliest dthrmov).
          3. Trigger validation: at least one trigger event with dthrmov <= first_88_dt.
          4. Eligible semi-join.
          5. Anti-join judgment (sit-17 guard: only valid MPU cod_tip_mov counts).
          6. Anti-join judmigracao.

        Returns: coddoc, dth_distribuicao, cod_classe (STRING), cod_sigilo
        """
        logger.info("Building dism (sit-88 based): cutoff=%s", cutoff_date)

        # Trigger codes per CNJ rule: 9, 24, 61, 65, 81
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

        # Step 5: anti-join judgment (sit-17 guard)
        judged_coddoc = (
            self._sit(self.JUDGMENT_CODES, self.DIST_START, cutoff_date)
            .filter(
                (col("situacao").cast("int") != 17)
                | col("cod_tip_mov").cast("int").isin(self.MPU_VALID_MOV_CODES)
            )
            .select("coddoc").distinct()
        )

        # Step 6: anti-join judmigracao (lost/migrated processes)
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

    # ── 2. primeirasentm ──────────────────────────────────────────────────────

    def build_primeirasentm(
        self, flow_start: date, period_end: date, eligible: DataFrame
    ) -> DataFrame:
        """First sentence per eligible process in [flow_start, period_end]."""
        logger.info("Building primeirasentm: %s → %s", flow_start, period_end)
        events = self._sit(self.PRIM_SENT_CODES, flow_start, period_end, eligible_df=eligible)
        return _first_per_coddoc(events).withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 3. baixm ──────────────────────────────────────────────────────────────

    def build_baixm(
        self, flow_start: date, period_end: date, eligible: DataFrame
    ) -> DataFrame:
        """First baixa per eligible process in [flow_start, period_end]."""
        logger.info("Building baixm: %s → %s", flow_start, period_end)
        events = self._sit(self.BAIXA_CODES, flow_start, period_end, eligible_df=eligible)
        return _first_per_coddoc(events).withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 4. mpum ───────────────────────────────────────────────────────────────

    def build_mpum(
        self, flow_start: date, period_end: date, eligible: DataFrame
    ) -> DataFrame:
        """First MPU/medida protetiva per eligible process in [flow_start, period_end]."""
        logger.info("Building mpum: %s → %s", flow_start, period_end)
        events = (
            self._sit(self.MPU_CODES, flow_start, period_end, eligible_df=eligible)
            .filter(col("cod_tip_mov").cast("int").isin(self.MPU_VALID_MOV_CODES))
        )
        return _first_per_coddoc(events).withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── 5. julgadom ───────────────────────────────────────────────────────────

    def build_julgadom(
        self,
        prim_df:  DataFrame,
        baixm_df: DataFrame,
        mpum_df:  DataFrame,
    ) -> DataFrame:
        """
        julgadom{NN} — first judgment per process after cutoff.

        Union(primeirasentm, baixm, mpum) → sit-17 guard → first per coddoc.
        """
        logger.info("Building julgadom: union(primeirasentm, baixm, mpum)")
        all_events = (
            prim_df.unionByName(baixm_df, allowMissingColumns=True)
                   .unionByName(mpum_df,  allowMissingColumns=True)
        )
        all_events = all_events.filter(
            (col("situacao") != 17)
            | col("cod_tip_mov").cast("int").isin(self.MPU_VALID_MOV_CODES)
        )
        return _first_per_coddoc(all_events, "dth_distribuicao")

    # ── 6. susm ───────────────────────────────────────────────────────────────

    def build_susm(
        self,
        flow_start:     date,
        period_end:     date,
        dism_coddoc:    DataFrame,
        julgadom_coddoc: DataFrame,
    ) -> DataFrame:
        """
        susm{NN} — processes from dism pool that are currently suspended at period_end.

        dism_pool − julgadom_pool → last suspension event per coddoc up to period_end
        → keep only ADD codes (currently suspended).
        """
        logger.info("Building susm: flow=%s → %s", flow_start, period_end)

        active_pool = dism_coddoc.join(julgadom_coddoc, on="coddoc", how="left_anti")

        all_susp = self.SUSPENSION_CODES + self.SUSPENSION_REMOVE_CODES
        _w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        last_susp = (
            self._sit(all_susp, self.PEND_START, period_end, eligible_df=active_pool)
            .withColumn("_rn", row_number().over(_w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        suspended_coddoc = (
            last_susp
            .filter(col("situacao").isin(self.SUSPENSION_CODES))
            .select("coddoc").distinct()
        )
        return suspended_coddoc

    # ── 7. desm ───────────────────────────────────────────────────────────────

    def build_desm(
        self,
        flow_start:      date,
        period_end:      date,
        dism_coddoc:     DataFrame,
        julgadom_coddoc: DataFrame,
    ) -> DataFrame:
        """
        desm{NN} — first levantamento de suspensão per process in flow window,
                   from the dism pool, excluding already-judged processes.

        Situacao-based approach: filter situacao IN SUSPENSION_REMOVE_CODES (20, 97–107, 138).
        The levantamento events in tb_tf_situacao_ejud are stored under their original
        EJUD situacao codes — NOT as cod_tip_mov MNI values (those are unpopulated).

        Pool:   dism{NN} (not _ant — these metas have no _ant branch)
        Anti-join: exclude julgadom{NN}
        Dedup:  first per coddoc (earliest dthrmov asc)

        Returns DataFrame: coddoc, dth_distribuicao, cod_classe, nivel_sigilo
        """
        logger.info("Building desm via situacao %s: flow=%s → %s",
                    self.SUSPENSION_REMOVE_CODES, flow_start, period_end)

        sit_tbl = self._tbl(self.db_situacao, _SITUACAO_TABLE)
        active_pool = dism_coddoc.join(julgadom_coddoc, on="coddoc", how="left_anti")

        events = (
            self.spark.table(sit_tbl)
            .filter(col("situacao").isin(self.SUSPENSION_REMOVE_CODES))
            .filter(col("dthrmov") >= lit(flow_start))
            .filter(col("dthrmov") <= lit(period_end))
            .select(
                col("coddoc"),
                col("dthrmov"),
                col("cod_classe").cast("string"),
                col("nivel_sigilo").alias("cod_sigilo"),
            )
            .join(active_pool.select("coddoc").distinct(), on="coddoc", how="semi")
        )

        final = _first_per_coddoc(events, "dthrmov")
        return final.withColumnRenamed("dthrmov", "dth_distribuicao")

    # ── Enrichment ────────────────────────────────────────────────────────────

    def _enrich_t_processo(self, df: DataFrame) -> DataFrame:
        """Left-join t_processo: num_processo, cod_competencia, cod_natureza, nivel_sigilo_derived."""
        tp_raw = self.spark.table(self._tbl(self.db_bronze, "t_processo"))
        tp_cols = [c.lower() for c in tp_raw.columns]

        # CNJ num_processo format: NNNNNNN-DD.AAAA.J.RR.LLLL
        # ejud t_processo columns: numsequencial, dv, numano, codjustica, codregi, codlocregi
        _NUM_PROC_COLS = {"numsequencial", "dv", "numano", "codjustica", "codregi", "codlocregi"}
        if _NUM_PROC_COLS.issubset(set(tp_cols)):
            from pyspark.sql.functions import concat
            num_proc_expr = concat(
                lpad(_norm(col("numsequencial")), 7, "0"),
                lit("-"),
                lpad(_norm(col("dv")), 2, "0"),
                lit("."),
                _norm(col("numano")),
                lit("."),
                _norm(col("codjustica")),
                lit("."),
                lpad(_norm(col("codregi")), 2, "0"),
                lit("."),
                lpad(_norm(col("codlocregi")), 4, "0"),
            )
        else:
            logger.info("t_processo missing num_processo columns — num_processo will be NULL")
            num_proc_expr = lit(None).cast("string")

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
        elif "codtipproc" in tp_cols:
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
                col("coddoc"), col("codtipproc"),
                num_proc_expr.alias("num_processo"),
                _norm(col("codcompt")).cast("int").alias("cod_competencia"),
                sigilo_expr,
            )
            tp = tp_base.join(tipoproc, on="codtipproc", how="left").drop("codtipproc")
        else:
            logger.info("t_processo has no codnat/codtipproc — cod_natureza will be NULL")
            tp = tp_raw.select(
                col("coddoc"),
                num_proc_expr.alias("num_processo"),
                _norm(col("codcompt")).cast("int").alias("cod_competencia"),
                lit(None).cast("int").alias("cod_natureza"),
                sigilo_expr,
            )

        return df.join(tp, on="coddoc", how="left")

    def _enrich_assunto(
        self, df: DataFrame, assunto_codes: Optional[List[int]] = None
    ) -> DataFrame:
        """
        Left-join processoassunto to get id_assunto_principal.

        Subject metas (04, 06-11):
            Uses self._assunto_raw pre-filtered in build_eligible_coddoc — no second scan.
            Ordering: (priority_rank ASC, numord ASC) when assunto_priority is set;
            numord ASC only for single-group metas (06, 07, 10, 11).

            Priority rank:
              WHEN codass IN assunto_priority[0] THEN 0   ← highest
              WHEN codass IN assunto_priority[1] THEN 1
              ...
              ELSE len(assunto_priority)                   ← safety fallback

        Non-subject metas (01, 02):
            No self._assunto_raw → fall back to numord=1 of the process's overall subjects.
        """
        try:
            if hasattr(self, "_assunto_raw") and self._assunto_raw is not None:
                # Subject meta fast path: reuse pre-filtered rows from build_eligible_coddoc.
                pa = self._assunto_raw.join(df.select("coddoc"), on="coddoc", how="semi")
                priority: Tuple[Tuple[int, ...], ...] = getattr(self, "_assunto_priority", ())
                if priority:
                    # Build priority_rank expression.
                    # reversed() ensures the lowest-index (highest-priority) group wins last
                    # in the WHEN chain, which is correct for `when(cond).otherwise(...)`.
                    n = len(priority)
                    rank_expr = lit(n)  # default: not in any known sub-group
                    for rank, subset in reversed(list(enumerate(priority))):
                        rank_expr = when(
                            col("codass_int").isin(list(subset)), lit(rank)
                        ).otherwise(rank_expr)
                    pa = pa.withColumn("priority_rank", rank_expr)
                    w = Window.partitionBy("coddoc").orderBy(
                        col("priority_rank").asc(), col("numord").asc()
                    )
                else:
                    # Single sub-group: lowest numord from the meta's code list.
                    w = Window.partitionBy("coddoc").orderBy(col("numord").asc())
                assunto = (
                    pa.withColumn("_rn", row_number().over(w))
                      .filter(col("_rn") == 1)
                      .select("coddoc", col("codass_int").alias("id_assunto_principal"))
                )
                return df.join(assunto, on="coddoc", how="left")

            # Fallback: non-subject metas (01, 02) — numord=1 of the overall subject list.
            pa = (
                self.spark.table(self._tbl(self.db_bronze, "processoassunto"))
                .join(df.select("coddoc"), on="coddoc", how="semi")
                .filter(col("numord").cast("int") == 1)
                .select("coddoc", _norm(col("codass")).cast("int").alias("id_assunto_principal"))
            )
            return df.join(pa, on="coddoc", how="left")
        except Exception as exc:
            logger.warning("Skipping assunto enrichment (%s)", exc)
            return df.withColumn("id_assunto_principal", lit(None).cast("int"))

    def _enrich_relator(self, df: DataFrame) -> DataFrame:
        """Left-join movimentorelator: cod_ult_magistrado + cod_ult_oj (last movement up to cutoff)."""
        try:
            relator_raw = (
                self.spark.table(self._tbl(self.db_bronze, "movimentorelator"))
                .join(df.select("coddoc"), on="coddoc", how="semi")
                .filter(col("dthrmov") <= self.cutoff_date)
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

            relator_cols = [c.lower() for c in relator_raw.columns]
            order_col = "dthrela" if "dthrela" in relator_cols else "dthrmov"

            # Handle both naming conventions: codmagrel (current) / codrelator (legacy)
            mag_col = "codmagrel" if "codmagrel" in relator_cols else "codrelator"
            # Handle both naming conventions: codorgjulg (current) / codoj (legacy)
            oj_col  = "codorgjulg" if "codorgjulg" in relator_cols else "codoj"

            _w = Window.partitionBy("coddoc").orderBy(col(order_col).desc())
            relator = (
                relator_raw
                .withColumn("_rn", row_number().over(_w))
                .filter(col("_rn") == 1).drop("_rn")
                .select(
                    col("coddoc"),
                    _norm(col(mag_col)).cast("int").alias("cod_ult_magistrado"),
                    _norm(col(oj_col)).cast("int").alias("cod_ult_oj"),
                )
            )
            return df.join(relator, on="coddoc", how="left")
        except Exception as exc:
            logger.warning("Skipping relator enrichment (%s)", exc)
            return (
                df.withColumn("cod_ult_magistrado", lit(None).cast("int"))
                  .withColumn("cod_ult_oj",         lit(None).cast("int"))
            )

    def _enrich(
        self, df: DataFrame, assunto_codes: Optional[List[int]] = None
    ) -> DataFrame:
        """Apply all enrichment steps (t_processo + assunto + relator)."""
        df = self._enrich_t_processo(df)
        df = self._enrich_assunto(df, assunto_codes=assunto_codes)
        df = self._enrich_relator(df)
        return df

    # ── Finalize → tb_ft_meta_analitico schema ───────────────────────────────

    def _finalize(
        self,
        df:       DataFrame,
        variavel: str,
        meta_ano: str,
        meta_cfg: MetaConfig,
    ) -> DataFrame:
        """Map enriched DataFrame to the exact tb_ft_meta_analitico DDL schema."""
        cod_ind: Optional[int] = meta_cfg.cod_indicadors.get(variavel)

        for c_name, c_type in [
            ("num_processo",         "string"),
            ("cod_competencia",      "int"),
            ("cod_natureza",         "int"),
            ("id_assunto_principal", "int"),
            ("cod_ult_magistrado",   "int"),
            ("cod_ult_oj",           "int"),
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
                col("dth_distribuicao").alias("dth_evento_indicador"),
                lit(cod_ind).cast("int").alias("cod_indicador"),
                coalesce(col("nivel_sigilo_derived"), lit(0)).alias("id_sigilo"),
                col("cod_natureza"),
                current_timestamp().alias("dth_atualizacao"),
                col("cod_ult_oj").cast("decimal(38,18)"),
                col("cod_ult_magistrado").cast("decimal(38,18)"),
                lit(int(meta_ano)).cast("int").alias("meta_ano"),
            )
        )

    # ── Orchestrate ───────────────────────────────────────────────────────────

    def run(
        self,
        year:         int,
        cutoff_date:  date,
        period_end:   date,
        meta_cfg:     MetaConfig,
        skip_dism:    bool = False,
    ) -> None:
        """
        Compute and write dism, julgadom, susm (and optionally desm) for the given assunto meta.

        flow_start = cutoff_date + 1 day  (e.g. cutoff=2025-12-31 → flow=2026-01-01)

        Args:
            skip_dism:  If True, skip the expensive eligible+dism+enrich steps and reload
                        dism_enriched from the already-written Iceberg partition.
                        Use on reruns when the stock (dism) is known to be stable.
        """
        meta_ano  = str(year)
        meta_num  = meta_cfg.meta_number
        flow_start = cutoff_date + timedelta(days=1)
        self.cutoff_date = cutoff_date  # needed by _enrich_relator

        logger.info("=" * 70)
        logger.info(
            "ejud_metacnj_mNN_emr  meta=%s  year=%d  cutoff=%s  flow_start=%s  "
            "period_end=%s  skip_dism=%s",
            meta_num, year, cutoff_date, flow_start, period_end, skip_dism,
        )
        logger.info("=" * 70)

        if not skip_dism:
            # ─ Step 1: eligible coddoc ───────────────────────────────────────
            # Also stores self._assunto_raw + self._assunto_priority for _enrich_assunto.
            eligible = self.build_eligible_coddoc(
                meta_cfg.assunto_codes,
                meta_cfg.assunto_priority,
            )
            eligible.cache()
            n_elig = eligible.count()
            logger.info("eligible coddoc cached — count=%d", n_elig)

            # ─ Step 2: dism stock ────────────────────────────────────────────
            dism = self.build_dism(cutoff_date, eligible)
            dism.cache()
            n_dism = dism.count()
            logger.info("dism%s cached — count=%d", meta_num, n_dism)
            eligible.unpersist()

            # ─ Step 3: enrich dism ───────────────────────────────────────────
            logger.info("Enriching dism%s (t_processo + assunto + relator)…", meta_num)
            dism_enriched = self._enrich(dism, assunto_codes=meta_cfg.assunto_codes)
            dism_enriched.cache()
            dism.unpersist()
        else:
            logger.info(
                "--skip-dism: reloading dism%s from Iceberg "
                "(year=%d, table=%s.%s.%s)…",
                meta_num, year, _CATALOG, _TARGET_DB, _TARGET_TABLE,
            )
            n_elig = n_dism = -1    # unknown (not recomputed)
            dism_enriched = _load_dism_from_table(self.spark, year, meta_cfg.v_dism(), meta_cfg.cod_indicadors[meta_cfg.v_dism()])
            dism_enriched.cache()
            n_dism = dism_enriched.count()
            logger.info("dism%s reloaded — count=%d", meta_num, n_dism)

        dism_coddoc = dism_enriched.select("coddoc").distinct()

        # ─ Step 4: julgadom sub-components ──────────────────────────────────
        prim = self.build_primeirasentm(flow_start, period_end, dism_coddoc)
        prim.cache()
        n_prim = prim.count()
        logger.info("primeirasentm%s cached — count=%d", meta_num, n_prim)

        baixm = self.build_baixm(flow_start, period_end, dism_coddoc)
        baixm.cache()
        n_baixm = baixm.count()
        logger.info("baixm%s cached — count=%d", meta_num, n_baixm)

        mpum = self.build_mpum(flow_start, period_end, dism_coddoc)
        mpum.cache()
        n_mpum = mpum.count()
        logger.info("mpum%s cached — count=%d", meta_num, n_mpum)

        julgadom = self.build_julgadom(prim, baixm, mpum)
        julgadom.cache()
        n_julgadom = julgadom.count()
        logger.info("julgadom%s cached — count=%d  (prim=%d baixm=%d mpum=%d)",
                    meta_num, n_julgadom, n_prim, n_baixm, n_mpum)
        prim.unpersist()
        baixm.unpersist()
        mpum.unpersist()

        julgadom_coddoc = julgadom.select("coddoc").distinct()

        # ─ Step 5: susm ──────────────────────────────────────────────────────
        susm_coddoc = self.build_susm(flow_start, period_end, dism_coddoc, julgadom_coddoc)
        susm_coddoc.cache()
        n_susm = susm_coddoc.count()
        logger.info("susm%s coddoc cached — count=%d", meta_num, n_susm)

        # ─ Step 6: desm ──────────────────────────────────────────────────────
        # desm only exists for metas that have it in cod_indicadors (06,07,08,10,11)
        # meta 04 has no desm (desm04_ant is a dim stub only)
        has_desm = meta_cfg.v_desm() in meta_cfg.cod_indicadors
        if has_desm:
            desm = self.build_desm(flow_start, period_end, dism_coddoc, julgadom_coddoc)
            desm.cache()
            n_desm = desm.count()
            logger.info("desm%s cached — count=%d", meta_num, n_desm)

        # ─ Step 7: write all variables ───────────────────────────────────────
        enrich_cols = [c for c in dism_enriched.columns
                       if c in ("num_processo", "cod_competencia", "cod_natureza",
                                "id_assunto_principal", "cod_ult_magistrado", "cod_ult_oj", "nivel_sigilo_derived")]
        enrich_base = dism_enriched.select("coddoc", *enrich_cols).distinct()
        # dism_dates used by susm and desm (coddoc-only DFs need dth_distribuicao + cod_classe)
        dism_dates = dism_enriched.select("coddoc", col("dth_distribuicao"), col("cod_classe"))

        # susm is coddoc-only and needs dth_distribuicao + cod_classe from dism_dates.
        # desm already has dth_distribuicao + cod_classe from build_desm — do NOT join dism_dates.
        _CODDOC_ONLY_VARS = {meta_cfg.v_susm()}

        variables_to_write = [
            (dism_enriched,  meta_cfg.v_dism()),
            (julgadom,       meta_cfg.v_julgadom()),
            (susm_coddoc,    meta_cfg.v_susm()),
        ]
        if has_desm:
            variables_to_write.append((desm, meta_cfg.v_desm()))

        enrich_cols_set = set(enrich_base.columns) - {"coddoc"}

        for df_var, var in variables_to_write:
            if skip_dism and var == meta_cfg.v_dism():
                logger.info("--skip-dism: skipping write for %s (already in table)", var)
                continue
            logger.info("Writing %s…", var)
            if var != meta_cfg.v_dism():
                # Drop any columns df_var already has that enrich_base will provide,
                # to avoid ambiguous reference errors (e.g. desm has cod_sigilo).
                overlap = [c for c in df_var.columns if c in enrich_cols_set]
                if overlap:
                    df_var = df_var.drop(*overlap)
                df_var = df_var.join(enrich_base, on="coddoc", how="left")
                # susm (coddoc-only) needs dth_distribuicao + cod_classe from dism
                if var in _CODDOC_ONLY_VARS:
                    df_var = df_var.join(dism_dates, on="coddoc", how="left")
            out = self._finalize(df_var, var, meta_ano, meta_cfg)
            _save_to_iceberg(
                self.spark,
                out,
                cod_ind=meta_cfg.cod_indicadors.get(var),
                sistema=self.IND_SISTEMA,
                meta_ano_int=year,
                variavel=var,
            )

        # ─ Cleanup ───────────────────────────────────────────────────────────
        to_unpersist = [dism_enriched, julgadom, susm_coddoc]
        if has_desm:
            to_unpersist.append(desm)
        for cached in to_unpersist:
            try:
                cached.unpersist()
            except Exception:
                pass

        logger.info("=" * 70)
        logger.info(
            "SUMMARY  meta=%s  dism=%d  julgadom=%d  susm=%d  desm=%s  skip_dism=%s",
            meta_num, n_dism, n_julgadom, n_susm,
            n_desm if has_desm else "N/A",
            skip_dism,
        )
        logger.info(
            "ejud_metacnj_mNN_emr  DONE  meta=%s  year=%d  cutoff=%s",
            meta_num, year, cutoff_date,
        )
        logger.info("=" * 70)


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args(argv=None):
    today = date.today()
    p = argparse.ArgumentParser(
        description="EJUD assunto-based metas (04,06,07,08,10,11) → Iceberg dw_2inst_ouro.tb_ft_meta_analitico"
    )
    p.add_argument(
        "--meta", required=True, choices=["04", "06", "07", "08", "10", "11"],
        help="Meta number: 04 | 06 | 07 | 08 | 10 | 11",
    )
    p.add_argument(
        "--year", type=int, default=today.year,
        help="Reference year written to meta_ano partition (default: current year)",
    )
    p.add_argument(
        "--cutoff-date", default=None,
        help="Stock cutoff date YYYY-MM-DD. "
             "Default: date(year - meta_offset, 12, 31) per meta. "
             "e.g. meta=10 year=2026 → 2025-12-31",
    )
    p.add_argument(
        "--period-end", default=None,
        help="End of flow window YYYY-MM-DD (default: YEAR-12-31)",
    )
    p.add_argument("--catalog",                  default=_CATALOG)
    p.add_argument("--db-situacao",              default=_DB_SITUACAO)
    p.add_argument("--db-prata",                 default=_DB_PRATA)
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
    p.add_argument(
        "--skip-dism", action="store_true", default=False,
        help="Skip the eligible+dism+enrich build steps and reload dism from the "
             "already-written Iceberg partition. Use on reruns when the stock is stable "
             "(dism is historical and won't change for past years).",
    )
    p.add_argument(
        "--config-path", default=_DEFAULT_CONFIG_PATH,
        help="S3 or local path to meta_config.json",
    )
    return p.parse_args(argv)


def main(argv=None):
    args    = _parse_args(argv)
    cfg      = _load_config(args.config_path)

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

    year     = args.year
    meta_num = int(args.meta)

    # Resolve cutoff default before creating SparkSession (reads config only)
    cutoff_offset = cfg[f"meta_{meta_num:02d}"]["cutoff_offset"]
    cutoff_date = (
        date.fromisoformat(args.cutoff_date)
        if args.cutoff_date
        else date(year - cutoff_offset, 12, 31)
    )
    period_end = (
        date.fromisoformat(args.period_end)
        if args.period_end
        else date(year, 12, 31)
    )

    logger.info("Meta config: %s | cutoff=%s | period_end=%s", args.meta, cutoff_date, period_end)

    spark = _create_spark_session(
        args.executor_memory,
        args.executor_memory_overhead,
        args.shuffle_partitions,
    )
    calc = MetaCNJAssuntoCalculator(
        spark=spark,
        cfg=cfg,
        meta_num=meta_num,
        catalog=args.catalog,
        db_situacao=args.db_situacao,
        db_bronze=args.db_bronze,
    )
    try:
        calc.run(
            year=year,
            cutoff_date=cutoff_date,
            period_end=period_end,
            meta_cfg=calc._meta_cfg,
            skip_dism=args.skip_dism,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
