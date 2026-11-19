#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_metacnj_relatorio_emr.py — CNJ Meta aggregated reports by OJ and Magistrado

Reads glue_catalog.dw_2inst_ouro.tb_ft_meta_analitico and produces two long-format
Iceberg tables:

  tb_ft_meta_por_oj         — one row per (cod_ult_oj, meta_ano, nm_meta, segmento)
  tb_ft_meta_por_magistrado — one row per (cod_ult_magistrado, meta_ano, nm_meta, segmento)

Output schema (9 columns):
  meta_ano   INT
  <group>    BIGINT   cod_ult_oj  or  cod_ult_magistrado
  nm_meta    STRING   'M01' | 'M02' | 'M02d' | 'M04' | 'M06' | 'M07a' | 'M07b' |
                      'M07c' | 'M08a' | 'M08b' | 'M10' | 'M11'
  segmento   STRING   '2inst' | 'turmas' | NULL
  n_julg     LONG     numerator (julgados)
  n_denom    LONG     effective denominator (after all subtractions)
  pct        DOUBLE   CNJ percentage (NULL when denominator ≤ 0)
  dth_carga  TIMESTAMP

Slices and formulas:
  M01           pct = julg / (cnm + des − sus)        × 100
  M02  2inst    pct = julg / (dism − sus)              × (1000/9)
  M02  turmas   pct = julg / (dism − sus)              × (1000/9.5)
  M02d 2inst    pct = julg / (dism − sus − des)        × 100         [antigos]
  M04  2inst    pct = julg / (dism − sus)              × 100
  M06           pct = julg / (dism − sus)              × (1000/5)
  M07a          pct = julg / (dism − sus)              × (1000/5)    [Indígenas]
  M07b          pct = julg / (dism − sus)              × (1000/5)    [Quilombolas]
  M07c          pct = julg / (dism − sus)              × (1000/5)    [Racismo/Injúria]
  M08a          pct = julg / (dism − sus)              × (1000/9)    [Viol. Doméstica]
  M08b          pct = julg / (dism − sus)              × (1000/7.5)  [Feminicídio]
  M10  2inst    pct = julg / (dism − sus)              × (1000/9.9)
  M11  2inst    pct = julg / (dism − sus)              × (1000/9.9)

Source  : glue_catalog.dw_2inst_ouro.tb_ft_meta_analitico
Targets : glue_catalog.dw_2inst_ouro.tb_ft_meta_por_oj
          glue_catalog.dw_2inst_ouro.tb_ft_meta_por_magistrado

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts/ejud_metacnj_relatorio_emr.py \\
      --year 2026 2>&1 | \\
      grep --line-buffered -E 'ejud_metacnj_relatorio|ERROR|Exception'
"""

import argparse
import logging
import sys
from datetime import date
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, round as _round, sum as _sum, when,
)
from pyspark.sql.types import LongType

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_metacnj_relatorio_emr")

# ── Infra ─────────────────────────────────────────────────────────────────────
_CATALOG    = "glue_catalog"
_WAREHOUSE  = "s3://sgdai-data-lake"
_DB_OURO    = "dw_2inst_ouro"
_SOURCE     = "tb_ft_meta_analitico"
_TARGET_OJ  = "tb_ft_meta_por_oj"
_TARGET_MAG = "tb_ft_meta_por_magistrado"

# ── Competência ───────────────────────────────────────────────────────────────
TURMAS    = [9, 10, 11, 14, 15, 19, 20, 22]
NOT_TO    = [1, 2, 12, 21]

# ── cod_indicador ─────────────────────────────────────────────────────────────
I_CNM01 = 1;  I_JULG01 = 2;  I_SUS01 = 3;  I_DES01 = 4
I_DISM02 = 6; I_JULG02 = 7;  I_SUS02 = 8
I_DES02_ANT = 9; I_JULG02_ANT = 10; I_DISM02_ANT = 11; I_SUS02_ANT = 12
I_DISM04 = 13; I_JULG04 = 14; I_SUS04 = 15
I_DISM06 = 20; I_JULG06 = 21; I_SUS06 = 22
I_DISM07 = 48; I_JULG07 = 49; I_SUS07 = 50
I_DISM08 = 27; I_JULG08 = 28; I_SUS08 = 29
I_DISM10 = 34; I_JULG10 = 35; I_SUS10 = 36
I_DISM11 = 41; I_JULG11 = 42; I_SUS11 = 43

# ── Meta 07 assunto codes ─────────────────────────────────────────────────────
M07_INDIGENA   = [9989, 12824, 13199, 9901, 15220, 10102, 3647, 15114, 10105,
                  10104, 10103, 15595, 15614, 15609, 15592, 15597, 15610, 15604,
                  15615, 15611]
M07_QUILOMBOLA = [12031, 12825, 15618, 15613]
M07_RACISMO    = [3613, 9873, 11849, 11844, 12543, 12861, 14101, 14102, 14100,
                  15134, 15135, 12547, 14111, 14110, 14108, 14697, 14699, 15127,
                  15128]

# ── Meta 08 assunto codes ─────────────────────────────────────────────────────
FEM_ASSUNTOS   = [12091, 12358]
VD_ASSUNTOS    = [10948, 10949, 11979, 12194, 12196, 14097, 14098, 14226, 14227,
                  14228, 14229, 14943, 14945, 14942, 14944, 15511]
VD_CLASSES_CNJ = [1268, 15309, 12423]   # classes that also count as VD


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(executor_memory="8g", executor_memory_overhead="4g",
                           shuffle_partitions=200) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudMetaCNJ_Relatorio")
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
def _ensure_iceberg_table(spark: SparkSession, fqn: str, location: str,
                           df: DataFrame) -> None:
    type_map = {
        "string": "STRING", "long": "BIGINT", "integer": "INT",
        "double": "DOUBLE", "timestamp": "TIMESTAMP", "timestamp_ntz": "TIMESTAMP",
    }
    col_defs = []
    for f in df.schema.fields:
        sql_t = type_map.get(str(f.dataType.typeName()).lower(), "STRING")
        col_defs.append(f"  `{f.name}` {sql_t}")
    non_part = [d for d in col_defs if not d.strip().startswith("`meta_ano`")]
    part_def = [d for d in col_defs if d.strip().startswith("`meta_ano`")]
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        + ",\n".join(non_part + part_def)
        + "\n)\nUSING iceberg\nPARTITIONED BY (meta_ano)\n"
        + f"LOCATION '{location}'\n"
        + "TBLPROPERTIES (\n"
        + "  'format-version' = '2',\n"
        + "  'write.parquet.compression-codec' = 'zstd'\n)"
    )
    spark.sql(ddl)


def _save(df: DataFrame, fqn: str, meta_ano: int) -> None:
    """Replace only one meta_ano slice so other years remain available."""
    logger.info("Deleting meta_ano=%s → %s", meta_ano, fqn)
    spark = df.sparkSession
    spark.sql(f"DELETE FROM {fqn} WHERE meta_ano = {meta_ano}")
    logger.info("Writing meta_ano=%s → %s", meta_ano, fqn)
    df.writeTo(fqn).option("write-audit-publish", "false").append()


# ── Slice builder (core) ──────────────────────────────────────────────────────

def _cnt(ind: int, extra=None):
    c = col("cod_indicador") == ind
    if extra is not None:
        c = c & extra
    return _sum(when(c, 1).otherwise(0))


def _build_slice(
    df: DataFrame,
    group_col: str,
    nm_meta: str,
    segmento: Optional[str],
    ind_julg: int,
    ind_dism: int,
    ind_sus: int,
    multiplier: float,
    ind_des: Optional[int] = None,       # subtracted from denom  (M02d)
    ind_cnm_add: Optional[int] = None,   # added to denom         (M01: cnm + des)
    row_filter=None,                     # competencia or subject Column condition
) -> DataFrame:
    """
    Build one long-format slice: one row per (group_col, meta_ano).

    Denominator rules:
      default       :  dism − sus
      ind_des set   :  dism − sus − des          (M02d)
      ind_cnm_add   :  dism + cnm_add − sus      (M01: dism=cnm01, cnm_add=des01)
    """
    # Pre-filter to only the relevant indicator codes — reduces data early
    all_inds = [ind_julg, ind_dism, ind_sus]
    if ind_des     is not None: all_inds.append(ind_des)
    if ind_cnm_add is not None: all_inds.append(ind_cnm_add)

    agg_exprs = [
        _cnt(ind_julg,    row_filter).alias("n_julg"),
        _cnt(ind_dism,    row_filter).alias("n_dism"),
        _cnt(ind_sus,     row_filter).alias("n_sus"),
    ]
    if ind_des     is not None:
        agg_exprs.append(_cnt(ind_des,     row_filter).alias("n_des"))
    if ind_cnm_add is not None:
        agg_exprs.append(_cnt(ind_cnm_add, row_filter).alias("n_cnm_add"))

    agg = (
        df.filter(col("cod_indicador").isin(all_inds))
        .groupBy(group_col, "meta_ano")
        .agg(*agg_exprs)
    )

    # Effective denominator expression
    if ind_cnm_add is not None:
        denom_expr = col("n_dism") + col("n_cnm_add") - col("n_sus")
    elif ind_des is not None:
        denom_expr = col("n_dism") - col("n_sus") - col("n_des")
    else:
        denom_expr = col("n_dism") - col("n_sus")

    return (
        agg
        .withColumn("n_denom", denom_expr)
        .withColumn("pct",
            when(col("n_denom") > 0,
                 _round(col("n_julg") / col("n_denom") * multiplier, 4))
            .otherwise(lit(None).cast("double"))
        )
        .withColumn("nm_meta",  lit(nm_meta))
        .withColumn("segmento", lit(segmento))
        .select(
            col(group_col),
            col("meta_ano"),
            col("nm_meta"),
            col("segmento"),
            col("n_julg").cast(LongType()),
            col("n_denom").cast(LongType()),
            col("pct"),
        )
    )


# ── Report builder ────────────────────────────────────────────────────────────

class MetaReportBuilder:

    def __init__(self, spark: SparkSession, catalog: str = _CATALOG,
                 db_ouro: str = _DB_OURO):
        self.spark   = spark
        self.catalog = catalog
        self.db_ouro = db_ouro

    def _fqn(self, table: str) -> str:
        return f"{self.catalog}.{self.db_ouro}.{table}"

    def _loc(self, table: str) -> str:
        return f"{_WAREHOUSE}/ouro/{self.db_ouro}/{table}"

    def _source(self, meta_ano: int) -> DataFrame:
        return (
            self.spark.table(self._fqn(_SOURCE))
            .filter(col("meta_ano") == lit(meta_ano))
        )

    def build(self, df: DataFrame, group_col: str) -> DataFrame:
        """
        Build the long-format report for one group key.
        Each meta slice reads only its relevant indicator codes,
        so no single shuffle touches the full table.
        """
        # ── Competência conditions ────────────────────────────────────────────
        is_2inst  = ~col("cod_competencia").isin(TURMAS + NOT_TO)
        is_turmas = col("cod_competencia").isin(TURMAS)

        # ── Meta 07 subject conditions (mutually exclusive) ───────────────────
        is_ind  = col("id_assunto_principal").isin(M07_INDIGENA)
        is_quil = col("id_assunto_principal").isin(M07_QUILOMBOLA)
        is_rac  = col("id_assunto_principal").isin(M07_RACISMO)

        # ── Meta 08 subject conditions ────────────────────────────────────────
        is_fem  = col("id_assunto_principal").isin(FEM_ASSUNTOS)
        is_vd   = (
            (col("id_assunto_principal").isin(VD_ASSUNTOS)
             | col("cod_classe_cnj").isin(VD_CLASSES_CNJ))
            & ~is_fem
        )

        # ── Build every slice ─────────────────────────────────────────────────
        # Segmento rule:
        #   M01     → both '2inst' and 'turmas' (user request)
        #   M02     → both '2inst' and 'turmas' (CNJ has separate targets b/c)
        #   M02d    → '2inst' only (antigos, no turmas variant)
        #   all others → '2inst' only; subject filters (M07/M08) are AND-ed with is_2inst
        slices: List[DataFrame] = [
            # M01 2inst: julg / (cnm + des − sus) × 100
            _build_slice(df, group_col, "M01",  "2inst",
                         ind_julg=I_JULG01, ind_dism=I_CNM01, ind_sus=I_SUS01,
                         multiplier=100.0, ind_cnm_add=I_DES01,
                         row_filter=is_2inst),

            # M01 turmas: julg / (cnm + des − sus) × 100
            _build_slice(df, group_col, "M01",  "turmas",
                         ind_julg=I_JULG01, ind_dism=I_CNM01, ind_sus=I_SUS01,
                         multiplier=100.0, ind_cnm_add=I_DES01,
                         row_filter=is_turmas),

            # M02 2inst: julg / (dism − sus) × (1000/9)
            _build_slice(df, group_col, "M02",  "2inst",
                         ind_julg=I_JULG02, ind_dism=I_DISM02, ind_sus=I_SUS02,
                         multiplier=1000.0 / 9.0, row_filter=is_2inst),

            # M02 turmas: julg / (dism − sus) × (1000/9.5)
            _build_slice(df, group_col, "M02",  "turmas",
                         ind_julg=I_JULG02, ind_dism=I_DISM02, ind_sus=I_SUS02,
                         multiplier=1000.0 / 9.5, row_filter=is_turmas),

            # M02d antigos 2inst: julg / (dism − sus − des) × 100
            _build_slice(df, group_col, "M02d", "2inst",
                         ind_julg=I_JULG02_ANT, ind_dism=I_DISM02_ANT,
                         ind_sus=I_SUS02_ANT, multiplier=100.0,
                         ind_des=I_DES02_ANT),

            # M04 2inst: julg / (dism − sus) × 100
            _build_slice(df, group_col, "M04",  "2inst",
                         ind_julg=I_JULG04, ind_dism=I_DISM04, ind_sus=I_SUS04,
                         multiplier=100.0, row_filter=is_2inst),

            # M06 2inst: julg / (dism − sus) × (1000/5)
            _build_slice(df, group_col, "M06",  "2inst",
                         ind_julg=I_JULG06, ind_dism=I_DISM06, ind_sus=I_SUS06,
                         multiplier=1000.0 / 5.0, row_filter=is_2inst),

            # M07a Indígenas 2inst: julg / (dism − sus) × (1000/5)
            _build_slice(df, group_col, "M07a", "2inst",
                         ind_julg=I_JULG07, ind_dism=I_DISM07, ind_sus=I_SUS07,
                         multiplier=1000.0 / 5.0, row_filter=is_ind & is_2inst),

            # M07b Quilombolas 2inst: julg / (dism − sus) × (1000/5)
            _build_slice(df, group_col, "M07b", "2inst",
                         ind_julg=I_JULG07, ind_dism=I_DISM07, ind_sus=I_SUS07,
                         multiplier=1000.0 / 5.0, row_filter=is_quil & is_2inst),

            # M07c Racismo / Injúria 2inst: julg / (dism − sus) × (1000/5)
            _build_slice(df, group_col, "M07c", "2inst",
                         ind_julg=I_JULG07, ind_dism=I_DISM07, ind_sus=I_SUS07,
                         multiplier=1000.0 / 5.0, row_filter=is_rac & is_2inst),

            # M08a Violência Doméstica 2inst: julg / (dism − sus) × (1000/9)
            _build_slice(df, group_col, "M08a", "2inst",
                         ind_julg=I_JULG08, ind_dism=I_DISM08, ind_sus=I_SUS08,
                         multiplier=1000.0 / 9.0, row_filter=is_vd & is_2inst),

            # M08b Feminicídio 2inst: julg / (dism − sus) × (1000/7.5)
            _build_slice(df, group_col, "M08b", "2inst",
                         ind_julg=I_JULG08, ind_dism=I_DISM08, ind_sus=I_SUS08,
                         multiplier=1000.0 / 7.5, row_filter=is_fem & is_2inst),

            # M10 2inst: julg / (dism − sus) × (1000/9.9)
            _build_slice(df, group_col, "M10",  "2inst",
                         ind_julg=I_JULG10, ind_dism=I_DISM10, ind_sus=I_SUS10,
                         multiplier=1000.0 / 9.9, row_filter=is_2inst),

            # M11 2inst: julg / (dism − sus) × (1000/9.9)
            _build_slice(df, group_col, "M11",  "2inst",
                         ind_julg=I_JULG11, ind_dism=I_DISM11, ind_sus=I_SUS11,
                         multiplier=1000.0 / 9.9, row_filter=is_2inst),
        ]

        result = slices[0]
        for s in slices[1:]:
            result = result.unionByName(s)

        # Filter out rows where both numerator and denominator are 0 (group key
        # had no activity for this meta at all — avoids noise in BI reports)
        result = result.filter((col("n_julg") > 0) | (col("n_denom") > 0))

        return result.withColumn("dth_carga", current_timestamp())

    def _discover_years(self) -> List[int]:
        """Return all distinct meta_ano values present in the source table."""
        rows = (
            self.spark.table(self._fqn(_SOURCE))
            .select("meta_ano").distinct()
            .orderBy("meta_ano")
            .collect()
        )
        return [r.meta_ano for r in rows]

    def _init_tables(self, sample_oj: DataFrame, sample_mag: DataFrame) -> None:
        """Ensure target tables exist without removing previously loaded years."""
        for fqn, loc, sample in [
            (self._fqn(_TARGET_OJ),  self._loc(_TARGET_OJ),  sample_oj),
            (self._fqn(_TARGET_MAG), self._loc(_TARGET_MAG), sample_mag),
        ]:
            _ensure_iceberg_table(self.spark, fqn, loc, sample)

    def run(self, years: Optional[List[int]] = None) -> None:
        if years is None:
            years = self._discover_years()
            logger.info("No --year supplied; processing all years found: %s", years)

        logger.info("=" * 70)
        logger.info("ejud_metacnj_relatorio_emr  years=%s", years)
        logger.info("=" * 70)

        tables_ready = False

        for meta_ano in years:
            logger.info("── meta_ano=%s ──", meta_ano)
            src = self._source(meta_ano).cache()
            rpt_oj  = self.build(src, "cod_ult_oj")
            rpt_mag = self.build(src, "cod_ult_magistrado")

            if not tables_ready:
                # First year: ensure target tables exist before scoped writes
                self._init_tables(rpt_oj, rpt_mag)
                tables_ready = True

            _save(rpt_oj,  self._fqn(_TARGET_OJ), meta_ano)
            _save(rpt_mag, self._fqn(_TARGET_MAG), meta_ano)
            src.unpersist()
            logger.info("meta_ano=%s written", meta_ano)

        logger.info("=" * 70)
        logger.info("ejud_metacnj_relatorio_emr  DONE  years=%s", years)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Build CNJ Meta reports (by OJ / Magistrado) from tb_ft_meta_analitico"
    )
    p.add_argument(
        "--year", type=int, default=None,
        help="Single meta_ano to process. Omit to process ALL years in the source table.",
    )
    p.add_argument("--catalog",                  default=_CATALOG)
    p.add_argument("--db-ouro",                  default=_DB_OURO)
    p.add_argument("--executor-memory",          default="8g")
    p.add_argument("--executor-memory-overhead", default="4g")
    p.add_argument("--shuffle-partitions", type=int, default=200)
    return p.parse_args(argv)


def main(argv=None):
    args  = _parse_args(argv)
    spark = _create_spark_session(
        executor_memory=args.executor_memory,
        executor_memory_overhead=args.executor_memory_overhead,
        shuffle_partitions=args.shuffle_partitions,
    )
    builder = MetaReportBuilder(
        spark=spark, catalog=args.catalog, db_ouro=args.db_ouro,
    )
    years = [args.year] if args.year is not None else None
    try:
        builder.run(years=years)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
