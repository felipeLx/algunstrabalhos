#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-shot historical reprocess: situação EJUD -> Iceberg (Glue Catalog)

Single SparkSession processes all months in [start_ym..end_ym] range.
Loops internally per YEAR (same movimento read scope), then filters
each month within that year. Eliminates 456 spark-submit startups.

Usage:
    spark-submit --master yarn --deploy-mode client \
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
      s3://sgdai-projetos/2instancia/2inst_situacao/ejud/scripts/ejud_situacao_historical.py \
      --start-ym 1988-01 --end-ym 2026-05 \
      --use-dim-integracao 2>&1 | \
      grep --line-buffered -E 'ejud_situacao_hist|ERROR|Exception'
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from functools import reduce
from typing import Dict, List, Optional, Tuple

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    broadcast, col, current_timestamp, date_format, lead, lit, lpad,
    months, regexp_replace, row_number, when,
)
from pyspark.sql.types import (
    IntegerType, StringType, TimestampType,
    StructField, StructType,
)
from pyspark.sql.window import Window

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_situacao_hist")


# ── Config ────────────────────────────────────────────────────────────────────
@dataclass
class JobConfig:
    catalog: str
    db_ejud: str
    db_prata: str
    target_table: str
    s3_json_prefix: str
    s3_warehouse: str
    driver_memory: str = "8g"
    executor_memory: str = "12g"
    executor_memory_overhead: str = "2g"
    shuffle_partitions: int = 100
    dry_run: bool = False
    use_dim_integracao: bool = False
    dim_integracao_catalog: str = "prata"
    dim_integracao_db: str = "dw_2inst_prata"
    dim_integracao_table: str = "tb_dm_integracao_ejud_cnj"


# ── SparkSession ──────────────────────────────────────────────────────────────
def create_spark_session(cfg: JobConfig) -> SparkSession:
    logger.info("Criando SparkSession (historical reprocess)")
    spark = (
        SparkSession.builder
        .appName("EjudSituacaoHistorical")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", cfg.s3_warehouse)
        .config("spark.sql.catalog.prata", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.prata.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.prata.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.prata.warehouse", "s3://sgdai-data-lake/prata")
        .config("spark.sql.defaultCatalog", cfg.catalog)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.executor.memory", cfg.executor_memory)
        .config("spark.executor.memoryOverhead", cfg.executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.io.compression.codec", "lz4")
        .config("spark.shuffle.compress", "true")
        .getOrCreate()
    )
    logger.info("Spark %s pronto", spark.version)
    return spark


# ── Helpers ───────────────────────────────────────────────────────────────────
def load_json_from_s3(s3_uri: str) -> object:
    without_prefix = s3_uri.removeprefix("s3://")
    bucket, key = without_prefix.split("/", 1)
    logger.info("Reading s3://%s/%s", bucket, key)
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def tbl(catalog: str, db: str, table: str) -> str:
    return f"{catalog}.{db}.{table}"


def str_to_bool(v: str) -> bool:
    return v.strip().lower() in {"1", "true", "yes", "t"}


def parse_ym(ym: str) -> Tuple[int, int]:
    parts = ym.split("-")
    return int(parts[0]), int(parts[1])


def generate_year_months(start_ym: Tuple[int, int], end_ym: Tuple[int, int]) -> List[Tuple[int, int]]:
    result = []
    y, m = start_ym
    while (y, m) <= end_ym:
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


# ── Main Job ──────────────────────────────────────────────────────────────────
class EjudSituacaoJob:

    def __init__(self, spark: SparkSession, cfg: JobConfig):
        self.spark = spark
        self.cfg = cfg

        s3p = cfg.s3_json_prefix.rstrip("/")
        self.situacoes: list = load_json_from_s3(f"{s3p}/situacoes.json")
        self.situacoes_start_end: list = load_json_from_s3(f"{s3p}/situacoes_start_end.json")
        self._mapping_df = self._build_mapping_df()

    # ── 1. Movement -> situacao mapping ───────────────────────────────────────
    def _build_mapping_df(self) -> DataFrame:
        records = []
        for entry in self.situacoes:
            sit_num = entry.get("situacao")
            for mov in entry.get("movimentos", []):
                cod_mov  = mov.get("cod_mni_movimento")
                comp1    = mov.get("complemento1")
                comp2    = mov.get("complemento2")
                comp3    = mov.get("complemento3")
                records.append((
                    int(cod_mov) if cod_mov is not None else None,
                    int(comp1)   if comp1   is not None else None,
                    int(comp2)   if comp2   is not None else None,
                    int(comp3)   if comp3   is not None else None,
                    int(sit_num) if sit_num is not None else None,
                ))

        schema = StructType([
            StructField("map_cod_mni_movimento", IntegerType(), True),
            StructField("map_complemento1",      IntegerType(), True),
            StructField("map_complemento2",      IntegerType(), True),
            StructField("map_complemento3",      IntegerType(), True),
            StructField("map_situacao",          IntegerType(), True),
        ])
        df = self.spark.createDataFrame(records, schema)
        logger.info("Mapping: %d rules loaded from situacoes.json", len(records))
        return df

    # ── 2. Read movements for a YEAR (shared across all months in that year) ──
    def _read_movimento_year(self, year: int) -> DataFrame:
        c, db = self.cfg.catalog, self.cfg.db_ejud
        return (
            self.spark.table(tbl(c, db, "movimento"))
            .filter(F.year(col("dthrmov")) == year)
            .filter(col("coddoc").isNotNull())
        )

    def _get_dim_table(self) -> DataFrame:
        return self.spark.table(
            tbl(self.cfg.dim_integracao_catalog, self.cfg.dim_integracao_db, self.cfg.dim_integracao_table)
        )

    def _get_t_proc_enriched(self) -> DataFrame:
        c, db = self.cfg.catalog, self.cfg.db_ejud
        t_processo = self.spark.table(tbl(c, db, "t_processo"))
        tipoprocesso = (
            self.spark.table(tbl(c, db, "tipoprocesso"))
            .select("codtipproc", col("codextcnj").alias("cod_classe"))
        )
        return (
            t_processo.select("coddoc", "indsigil", "codtipproc")
            .join(broadcast(tipoprocesso), on="codtipproc", how="left")
            .select("coddoc", "indsigil", "cod_classe")
        )

    def _resolve_cnj_codes_dim(self, movimento: DataFrame) -> DataFrame:
        dim = self._get_dim_table()
        t_proc_enriched = self._get_t_proc_enriched()

        dim_join = (
            (col("m.codfase") == col("dim.codfase"))
            & (col("dim.codcompl1").isNull() | (col("m.codcompl1") == col("dim.codcompl1")))
            & (col("dim.codcompl2").isNull() | (col("m.codcompl2") == col("dim.codcompl2")))
            & (col("dim.codcompl3").isNull() | (col("m.codcompl3") == col("dim.codcompl3")))
        )

        df_mov = (
            movimento.alias("m")
            .join(broadcast(dim).alias("dim"), dim_join, "left")
            .join(broadcast(t_proc_enriched), on="coddoc", how="left")
            .withColumn(
                "_dim_specificity",
                when(col("dim.codcompl1").isNotNull(), 1).otherwise(0)
                + when(col("dim.codcompl2").isNotNull(), 1).otherwise(0)
                + when(col("dim.codcompl3").isNotNull(), 1).otherwise(0),
            )
            .select(
                col("m.coddoc"),
                col("m.dthrmov").alias("dth_inicio"),
                col("m.codfase"),
                col("m.codcompl1"),
                col("m.codcompl2"),
                col("m.codcompl3"),
                col("dim.cod_mni_cnj").alias("cod_mni_movimento_int"),
                col("dim.base_cod_mni"),
                col("dim.cnj_comp1"),
                col("dim.cnj_comp2"),
                col("dim.ultimonivelcnj"),
                col("indsigil"),
                col("cod_classe"),
                col("_dim_specificity"),
            )
        )

        w_dim = Window.partitionBy("coddoc", "dth_inicio", "codfase").orderBy(
            col("_dim_specificity").desc()
        )
        return (
            df_mov
            .withColumn("_rn", row_number().over(w_dim))
            .filter(col("_rn") == 1)
            .drop("_rn", "_dim_specificity")
        )

    def _resolve_cnj_codes_legacy(self, movimento: DataFrame) -> DataFrame:
        c, db = self.cfg.catalog, self.cfg.db_ejud

        _faseext_overrides = {60006: 3, 60005: 11009}
        faseexterna = self.spark.table(tbl(c, db, "faseexterna")).withColumn(
            "codfaseext",
            reduce(
                lambda c, kv: when(c == lit(kv[0]), lit(kv[1])).otherwise(c),
                _faseext_overrides.items(),
                col("codfaseext"),
            ),
        )

        integ = (
            self.spark.table(tbl(c, db, "integracaofasecompl_ejud_cnj"))
            .filter(col("indativo") == "S")
        )

        t_proc_enriched = self._get_t_proc_enriched()

        join_cond = (
            (col("fe.codfaseext") == col("i.codfasenivel2"))
            & (
                (col("i.ultimonivelcnj") < 3)
                | (col("m.codcompl1") == col("i.codcompl1nivel3"))
            )
            & (
                (col("i.ultimonivelcnj") < 4)
                | (col("m.codcompl2") == col("i.codcompl2nivel4"))
            )
            & (
                (col("i.ultimonivelcnj") < 5)
                | (col("m.codcompl3") == col("i.codcompl3nivel5"))
            )
        )

        return (
            movimento.alias("m")
            .join(broadcast(faseexterna).alias("fe"), col("m.codfase") == col("fe.codfase"), "left")
            .join(broadcast(integ).alias("i"), join_cond, "left")
            .join(broadcast(t_proc_enriched), on="coddoc", how="left")
            .select(
                col("m.coddoc"),
                col("m.dthrmov").alias("dth_inicio"),
                col("m.codfase"),
                col("m.codcompl1"),
                col("m.codcompl2"),
                col("m.codcompl3"),
                when(
                    col("i.codcompl1nivel3cnj").isNotNull(),
                    col("i.codcompl1nivel3cnj"),
                ).otherwise(col("i.codfasenivel2cnj")).alias("cod_mni_movimento_int"),
                when(
                    col("i.codfasenivel2cnj").isNotNull(),
                    col("i.codfasenivel2cnj"),
                ).otherwise(col("fe.codfaseext")).alias("base_cod_mni"),
                col("i.codcompl2nivel4cnj").alias("cnj_comp1"),
                col("i.codcompl3nivel5cnj").alias("cnj_comp2"),
                col("i.ultimonivelcnj"),
                col("indsigil"),
                col("cod_classe"),
            )
        )

    def _apply_situacao_mapping(self, df_mov: DataFrame, year: int, month: int) -> DataFrame:
        mapping_clean = (
            self._mapping_df
            .withColumn("map_complemento1",
                when(col("map_complemento1").cast("string") == "", None)
                .otherwise(col("map_complemento1")))
            .withColumn("map_complemento2",
                when(col("map_complemento2").cast("string") == "", None)
                .otherwise(col("map_complemento2")))
        )

        map_join_cond = (
            (col("d.cod_mni_movimento_int") == col("mp.map_cod_mni_movimento"))
            & (col("mp.map_complemento1").isNull()
               | (col("d.cnj_comp1") == col("mp.map_complemento1")))
            & (col("mp.map_complemento2").isNull()
               | (col("d.cnj_comp2") == col("mp.map_complemento2")))
        )

        df_pass1 = (
            df_mov.alias("d")
            .join(broadcast(mapping_clean).alias("mp"), map_join_cond, "left")
            .withColumn("_specificity",
                when(col("mp.map_complemento1").isNotNull(), 1).otherwise(0)
                + when(col("mp.map_complemento2").isNotNull(), 1).otherwise(0))
            .select("d.*", col("mp.map_situacao").alias("situacao"), "_specificity")
        )

        df_matched = df_pass1.filter(col("situacao").isNotNull())
        df_unmatched = df_pass1.filter(col("situacao").isNull()).drop("situacao", "_specificity")

        fallback_cond = (
            (col("u.base_cod_mni") == col("mp2.map_cod_mni_movimento"))
            & (
                col("u.cod_mni_movimento_int").isNull()
                | (col("u.base_cod_mni") != col("u.cod_mni_movimento_int"))
            )
            & (col("mp2.map_complemento1").isNull())
            & (col("mp2.map_complemento2").isNull())
        )
        df_fallback = (
            df_unmatched.alias("u")
            .join(broadcast(mapping_clean).alias("mp2"), fallback_cond, "inner")
            .select("u.*", col("mp2.map_situacao").alias("situacao"), F.lit(0).alias("_specificity"))
        )

        df_mapped = df_matched.unionByName(df_fallback)

        w_map = Window.partitionBy("coddoc", "dth_inicio", "codfase").orderBy(
            col("_specificity").desc(), col("situacao").asc()
        )
        df_mapped = (
            df_mapped
            .withColumn("_rn", row_number().over(w_map))
            .filter(col("_rn") == 1)
            .drop("_rn", "_specificity")
        )

        w_dedup = Window.partitionBy("coddoc", "dth_inicio", "situacao").orderBy(
            col("codfase").asc_nulls_last()
        )
        df_deduped = (
            df_mapped
            .withColumn("_rn", row_number().over(w_dedup))
            .filter(col("_rn") == 1)
            .drop("_rn", "base_cod_mni")
        )

        df_month = df_deduped.filter(
            (F.year(col("dth_inicio")) == year)
            & (F.month(col("dth_inicio")) == month)
        )

        return df_month

    # ── 3. dth_fim ────────────────────────────────────────────────────────────
    def compute_dth_fim(self, df: DataFrame) -> DataFrame:
        logger.info("Computing dth_fim")
        data = self.situacoes_start_end

        instantaneous = [
            str(s["situacao"]) for s in data
            if (len(s.get("start_with", [])) == 1
                and s.get("start_with") == s.get("end_with"))
        ]
        self_closing = [
            str(s["situacao"]) for s in data
            if not s.get("start_with") and not s.get("end_with")
        ]
        end_with_map: Dict[str, List[str]] = {
            str(s["situacao"]): [str(e) for e in s["end_with"]]
            for s in data if s.get("end_with")
        }
        group_end_map: Dict[str, List[str]] = {
            str(s["situacao"]): [str(e) for e in s["group_end_with"]]
            for s in data if s.get("group_end_with")
        }

        w = Window.partitionBy("coddoc").orderBy("dth_inicio")
        df = (
            df
            .withColumn("_sit_str",  col("situacao").cast("string"))
            .withColumn("_next_sit", lead("_sit_str").over(w))
            .withColumn("_next_dth", lead("dth_inicio").over(w))
        )

        df = df.withColumn("_dth_inst",
            when(col("_sit_str").isin(instantaneous + self_closing), col("dth_inicio")))

        end_with_rows = [
            (sit, end) for sit, ends in end_with_map.items() for end in ends
        ]
        if end_with_rows:
            ew_df = self.spark.createDataFrame(end_with_rows, ["_ew_sit", "_ew_nxt"])
            df = (
                df
                .join(broadcast(ew_df),
                      (col("_sit_str") == col("_ew_sit"))
                      & (col("_next_sit") == col("_ew_nxt")),
                      "left")
                .withColumn("_dth_end",
                    when(
                        col("_ew_sit").isNotNull()
                        & ~((col("_sit_str") == col("_next_sit"))
                            & (col("_next_dth") <= col("dth_inicio"))),
                        col("_next_dth")))
                .drop("_ew_sit", "_ew_nxt")
            )
        else:
            df = df.withColumn("_dth_end", lit(None).cast(TimestampType()))

        group_end_rows = [
            (sit, end) for sit, ends in group_end_map.items() for end in ends
        ]
        if group_end_rows:
            ge_df = self.spark.createDataFrame(group_end_rows, ["_ge_sit", "_ge_nxt"])
            df = (
                df
                .join(broadcast(ge_df),
                      (col("_sit_str") == col("_ge_sit"))
                      & (col("_next_sit") == col("_ge_nxt")),
                      "left")
                .withColumn("_dth_grp",
                    when(col("_ge_sit").isNotNull(), col("_next_dth")))
                .drop("_ge_sit", "_ge_nxt")
            )
        else:
            df = df.withColumn("_dth_grp", lit(None).cast(TimestampType()))

        df = df.withColumn(
            "dth_fim",
            when(col("_dth_inst").isNotNull(), col("_dth_inst"))
            .when(col("_dth_end").isNotNull(), col("_dth_end"))
            .when(col("_dth_grp").isNotNull(), col("_dth_grp"))
            .otherwise(lit(None).cast(TimestampType())),
        ).drop("_sit_str", "_next_sit", "_next_dth", "_dth_inst", "_dth_end", "_dth_grp")

        return df

    # ── 4. Special situacoes 025 / 088 ───────────────────────────────────────
    def process_special_situacoes(self, df: DataFrame) -> Optional[DataFrame]:
        logger.info("Processing special situacoes 025 and 088")

        sit_dict = {
            str(s["situacao"]): {
                "start_with": [str(x) for x in s.get("start_with", [])],
                "end_with":   [str(x) for x in s.get("end_with", [])],
            }
            for s in self.situacoes_start_end if "situacao" in s
        }

        results = []
        w_ordered = Window.partitionBy("coddoc").orderBy("dth_inicio")

        for sit_key, sit_code in [("25", 25), ("88", 88)]:
            if sit_key not in sit_dict:
                continue
            cfg_sit    = sit_dict[sit_key]
            start_ints = [int(x) for x in cfg_sit["start_with"]]
            end_ints   = [int(x) for x in cfg_sit["end_with"]]
            all_ints   = list(set(start_ints + end_ints))

            df_marked = (
                df
                .withColumn("_is_start",  col("situacao").isin(start_ints))
                .withColumn("_is_closer", col("situacao").isin(all_ints))
                .withColumn("_next_is_closer", lead(col("situacao").isin(all_ints)).over(w_ordered))
                .withColumn("_next_dth",        lead(col("dth_inicio")).over(w_ordered))
            )

            df_special = (
                df_marked
                .filter(col("_is_start"))
                .withColumn(
                    "dth_fim",
                    when(col("_next_is_closer"), col("_next_dth"))
                    .otherwise(lit(None).cast(TimestampType())),
                )
                .filter(
                    col("dth_fim").isNull()
                    | (col("dth_fim") > col("dth_inicio"))
                )
                .withColumn("situacao", lit(sit_code))
                .drop("_is_start", "_is_closer", "_next_is_closer", "_next_dth")
            )

            logger.info("Situacao %d: queued for write", sit_code)
            results.append(df_special)

        if not results:
            return None
        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), results)

    # ── 5. Write to Iceberg ───────────────────────────────────────────────────
    def _table_exists(self, full_target: str) -> bool:
        try:
            self.spark.sql(f"DESCRIBE TABLE {full_target}")
            return True
        except Exception:
            return False

    def _align_schema(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("dthrmov",           col("dth_inicio"))
            .withColumn("ident_mov",         col("cod_mni_movimento_int").cast("bigint"))
            .withColumn("nivel_sigilo",      col("indsigil").cast(IntegerType()))
            .withColumn("cod_oj",            lit(None).cast(StringType()))
            .withColumn("cod_tip_mov",       col("cod_mni_movimento_int").cast(StringType()))
            .withColumn("cod_tip_mov_numero", col("cod_mni_movimento_int").cast(IntegerType()))
            .withColumn("situacao_partition", lpad(col("situacao").cast(StringType()), 3, "0"))
            .withColumn("year_month",        date_format(col("dth_inicio"), "yyyy-MM"))
            .withColumn("data_carga",        current_timestamp())
            .drop("dth_inicio", "codfase", "codcompl1", "codcompl2", "codcompl3",
                  "cod_mni_movimento_int", "cnj_comp1", "cnj_comp2", "cnj_comp3",
                  "ultimonivelcnj", "indsigil")
        )

    def save_to_iceberg(self, df: DataFrame) -> None:
        full_target = tbl(self.cfg.catalog, self.cfg.db_prata, self.cfg.target_table)
        if not self._table_exists(full_target):
            logger.info("Table not found — creating: %s", full_target)
            (
                df.writeTo(full_target)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty("write_compression", "zstd")
                .partitionedBy(months(col("dthrmov")))
                .create()
            )
        else:
            df.writeTo(full_target).overwritePartitions()

    def append_to_iceberg(self, df: DataFrame) -> None:
        full_target = tbl(self.cfg.catalog, self.cfg.db_prata, self.cfg.target_table)
        df.writeTo(full_target).append()

    # ── 6. Process single month (from pre-read year data) ────────────────────
    def process_month(self, df_cnj_year: DataFrame, year: int, month: int) -> None:
        logger.info("Processing %d-%02d", year, month)
        t0 = time.time()

        df_month = self._apply_situacao_mapping(df_cnj_year, year, month)

        # Main situacoes (exclude 25/88)
        df_main = df_month.filter(~col("situacao").isin([25, 88]))
        df_main = self.compute_dth_fim(df_main)
        df_main = self._align_schema(df_main)

        if self.cfg.dry_run:
            df_main.groupBy("year_month", "situacao").count().orderBy("year_month", "situacao").show(50, truncate=False)
        else:
            self.save_to_iceberg(df_main)

        # Special situacoes 025/088
        df_special = self.process_special_situacoes(df_month)
        if df_special is not None:
            df_special = self._align_schema(df_special)
            if not self.cfg.dry_run:
                self.append_to_iceberg(df_special)

        elapsed = time.time() - t0
        logger.info("Done %d-%02d in %.1fs", year, month, elapsed)

    # ── 7. Orchestrate: loop years, months within year ────────────────────────
    def run_range(self, start_ym: Tuple[int, int], end_ym: Tuple[int, int]) -> None:
        all_months = generate_year_months(start_ym, end_ym)
        total = len(all_months)
        logger.info("=" * 70)
        logger.info("Historical reprocess: %d-%02d to %d-%02d (%d months)",
                    start_ym[0], start_ym[1], end_ym[0], end_ym[1], total)
        logger.info("=" * 70)

        # Group months by year
        years_months: Dict[int, List[int]] = {}
        for y, m in all_months:
            years_months.setdefault(y, []).append(m)

        processed = 0
        t_start = time.time()

        for year in sorted(years_months.keys()):
            months_in_year = sorted(years_months[year])
            logger.info("─" * 50)
            logger.info("YEAR %d: months %s", year, months_in_year)
            logger.info("─" * 50)

            # Read movimento ONCE per year
            t_year = time.time()
            movimento = self._read_movimento_year(year)

            # Resolve CNJ codes ONCE per year (expensive joins)
            if self.cfg.use_dim_integracao:
                logger.info("Resolving CNJ codes via dim_integracao for year %d", year)
                df_cnj_year = self._resolve_cnj_codes_dim(movimento)
            else:
                logger.info("Resolving CNJ codes via legacy chain for year %d", year)
                df_cnj_year = self._resolve_cnj_codes_legacy(movimento)

            # Cache year data — reused across all months in this year
            df_cnj_year.cache()
            logger.info("Year %d data cached, processing %d months", year, len(months_in_year))

            for month in months_in_year:
                self.process_month(df_cnj_year, year, month)
                processed += 1
                elapsed_total = time.time() - t_start
                avg_per_month = elapsed_total / processed
                remaining = (total - processed) * avg_per_month
                logger.info(
                    "Progress: %d/%d (%.1f%%) | ETA: %.0f min",
                    processed, total, 100 * processed / total, remaining / 60,
                )

            # Release year cache
            df_cnj_year.unpersist()
            logger.info("Year %d completed in %.1fs", year, time.time() - t_year)

        total_time = time.time() - t_start
        logger.info("=" * 70)
        logger.info("ALL DONE: %d months in %.1f min (%.1f h)",
                    total, total_time / 60, total_time / 3600)
        logger.info("=" * 70)


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Historical reprocess situação EJUD")
    p.add_argument("--start-ym", required=True, help="Start year-month (e.g. 1988-01)")
    p.add_argument("--end-ym",   required=True, help="End year-month (e.g. 2026-05)")
    p.add_argument("--catalog",      default="glue_catalog")
    p.add_argument("--db-ejud",      default="ejud")
    p.add_argument("--db-prata",     default="dw_2inst_ouro")
    p.add_argument("--target-table", default="tb_tf_situacao_ejud")
    p.add_argument("--s3-json-prefix",
                   default="s3://sgdai-projetos/2instancia/2inst_situacao/ejud/json")
    p.add_argument("--s3-warehouse",
                   default="s3://sgdai-data-lake/ouro")
    p.add_argument("--driver-memory",            default="8g")
    p.add_argument("--executor-memory",          default="12g")
    p.add_argument("--executor-memory-overhead", default="2g")
    p.add_argument("--shuffle-partitions", type=int, default=100)
    p.add_argument("--dry-run", default="false")
    p.add_argument("--use-dim-integracao", action="store_true",
                   help="Use tb_dm_integracao_ejud_cnj instead of legacy triple-join")
    p.add_argument("--dim-integracao-catalog", default="prata")
    p.add_argument("--dim-integracao-db", default="dw_2inst_prata")
    p.add_argument("--dim-integracao-table", default="tb_dm_integracao_ejud_cnj")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = JobConfig(
        catalog=args.catalog,
        db_ejud=args.db_ejud,
        db_prata=args.db_prata,
        target_table=args.target_table,
        s3_json_prefix=args.s3_json_prefix,
        s3_warehouse=args.s3_warehouse,
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
        executor_memory_overhead=args.executor_memory_overhead,
        shuffle_partitions=args.shuffle_partitions,
        dry_run=str_to_bool(args.dry_run),
        use_dim_integracao=args.use_dim_integracao,
        dim_integracao_catalog=args.dim_integracao_catalog,
        dim_integracao_db=args.dim_integracao_db,
        dim_integracao_table=args.dim_integracao_table,
    )

    spark = create_spark_session(cfg)
    try:
        start_ym = parse_ym(args.start_ym)
        end_ym = parse_ym(args.end_ym)
        EjudSituacaoJob(spark, cfg).run_range(start_ym, end_ym)
    except Exception as exc:
        logger.error("Job falhou: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
