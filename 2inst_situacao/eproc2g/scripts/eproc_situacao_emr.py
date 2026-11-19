#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga mensal de situação EPROC -> Iceberg (Glue Catalog)

Single-script design: no external .py dependencies.
JSON mapping files (situacoes.json / situacoes_start_end.json) are read
directly from S3 using boto3 at startup.

NOTE: Iceberg/Glue Catalog Spark configs are NOT set here — they are
already injected at cluster level via emr-config.json (Classification:
spark-defaults). Duplicating them here caused SparkSession startup hangs.

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_situacao/eproc2g/scripts/eproc_situacao_emr.py \\
      --year 2026 --month 3 2>&1 | \\
      grep --line-buffered -E 'eproc_situacao_emr|ERROR|Exception'
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from functools import reduce
from typing import Dict, List, Optional

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    broadcast, col, date_format, lead, lit, months,
    regexp_replace, row_number, when,
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
logger = logging.getLogger("eproc_situacao_emr")


# ── Config ────────────────────────────────────────────────────────────────────
@dataclass
class JobConfig:
    catalog: str
    db_eproc: str
    db_prata: str
    target_table: str
    s3_json_prefix: str
    s3_warehouse: str
    driver_memory: str = "8g"
    executor_memory: str = "12g"
    executor_memory_overhead: str = "2g"
    shuffle_partitions: int = 100
    dry_run: bool = False


# ── SparkSession ──────────────────────────────────────────────────────────────
def create_spark_session(cfg: JobConfig) -> SparkSession:
    logger.info("Criando SparkSession")
    spark = (
        SparkSession.builder
        .appName("EprocSituacaoEMR")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", cfg.s3_warehouse)
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
    """Read a JSON file from S3 and return parsed content."""
    without_prefix = s3_uri.removeprefix("s3://")
    bucket, key = without_prefix.split("/", 1)
    logger.info("Reading s3://%s/%s", bucket, key)
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def tbl(catalog: str, db: str, table: str) -> str:
    return f"{catalog}.{db}.{table}"


def str_to_bool(v: str) -> bool:
    return v.strip().lower() in {"1", "true", "yes", "t"}


# ── Main Job ──────────────────────────────────────────────────────────────────
class EprocSituacaoJob:

    def __init__(self, spark: SparkSession, cfg: JobConfig):
        self.spark = spark
        self.cfg = cfg
        self._id_filter_cache: Optional[DataFrame] = None
        self._eventos_cache: Optional[DataFrame] = None

        s3p = cfg.s3_json_prefix.rstrip("/")
        self.situacoes: list = load_json_from_s3(f"{s3p}/situacoes.json")  # type: ignore
        self.situacoes_start_end: list = load_json_from_s3(f"{s3p}/situacoes_start_end.json")  # type: ignore
        self._mapping_df = self._build_mapping_df()

    # ── 1. Movement -> situacao mapping ───────────────────────────────────────
    def _build_mapping_df(self) -> DataFrame:
        """Build a broadcast-ready DataFrame from situacoes.json."""
        records = []
        for entry in self.situacoes:
            sit_num = entry.get("situacao")
            for mov in entry.get("movimentos", []):
                cod_mov      = mov.get("cod_mni_movimento")
                cod_comp     = mov.get("cod_mni_complemento")
                cod_comp_tab = mov.get("cod_mni_complemento_tabelado")
                records.append((
                    int(cod_mov)      if cod_mov      is not None else None,
                    int(cod_comp)     if cod_comp     is not None else None,
                    int(cod_comp_tab) if cod_comp_tab is not None else None,
                    int(sit_num)      if sit_num      is not None else None,
                ))

        schema = StructType([
            StructField("map_cod_mni_movimento",           IntegerType(), True),
            StructField("map_cod_mni_complemento",         IntegerType(), True),
            StructField("map_cod_mni_complemento_tabelado",IntegerType(), True),
            StructField("map_situacao",                    IntegerType(), True),
        ])
        df = self.spark.createDataFrame(records, schema)
        logger.info("Mapping: %d rules loaded from situacoes.json", len(records))
        return df

    # ── 2. Read movements ─────────────────────────────────────────────────────
    def read_movements(self, year: int, month: int) -> DataFrame:
        """Single-pass join — AQE handles skew and join ordering at runtime."""
        logger.info("Reading EPROC movements from Glue catalog for %d-%02d", year, month)
        c, db = self.cfg.catalog, self.cfg.db_eproc

        processo_evento = (
            self.spark.table(tbl(c, db, "processo_evento"))
            .filter(F.year(col("dth_evento")) == year)
            .filter(col("dth_desativacao").isNull())
            .filter(col("id_evento_judicial") != 40400400)
        )

        evento_judicial = self.spark.table(tbl(c, db, "evento_judicial"))
        ej_comp_tab     = self.spark.table(tbl(c, db, "evento_judicial_mni_complemento_tabelado"))
        mni_comp_tab    = self.spark.table(tbl(c, db, "mni_complemento_tabelado"))
        processo        = self.spark.table(tbl(c, db, "processo"))
        relacao_cj_mni  = self.spark.table(tbl(c, db, "relacao_classe_judicial_mni"))

        df_raw = (
            processo_evento
            .join(broadcast(evento_judicial),
                  processo_evento["id_evento_judicial"] == evento_judicial["id_evento_judicial"],
                  "inner")
            .join(ej_comp_tab,
                  evento_judicial["id_evento_judicial"] == ej_comp_tab["id_evento_judicial"],
                  "left")
            .join(broadcast(mni_comp_tab),
                  ej_comp_tab["cod_mni_complemento_tabelado"] == mni_comp_tab["cod_mni_complemento_tabelado"],
                  "left")
            .join(processo, on="id_processo", how="inner")
            .join(broadcast(relacao_cj_mni),
                  processo["id_classe_judicial"] == relacao_cj_mni["id_classe_judicial"],
                  "left")
            .select(
                processo_evento["id_processo"],
                processo["num_processo"],
                processo["id_orgao_colegiado"],
                processo["id_orgao_juizo"],
                relacao_cj_mni["cod_mni_classe_judicial"].alias("id_classe_judicial"),
                processo["id_assunto_principal"],
                processo["id_sigilo"],
                processo_evento["dth_evento"].alias("dth_inicio"),
                evento_judicial["id_evento_judicial"],
                evento_judicial["cod_mni_movimento"].alias("cod_mni_movimento_raw"),
                ej_comp_tab["cod_mni_complemento_tabelado"],
                mni_comp_tab["cod_mni_complemento"],
                processo_evento["id_processo_evento"],
            )
        )

        df_selected = df_raw.withColumn(
            "cod_mni_movimento",
            regexp_replace(col("cod_mni_movimento_raw").cast(StringType()), r"\.0$", ""),
        ).drop("cod_mni_movimento_raw")

        # Prepare int columns for mapping join
        df_for_map = (
            df_selected
            .withColumn("_mov_int",      col("cod_mni_movimento").cast(IntegerType()))
            .withColumn("_comp_int",     col("cod_mni_complemento").cast(IntegerType()))
            .withColumn("_comp_tab_int", col("cod_mni_complemento_tabelado").cast(IntegerType()))
        )

        # Sanitize mapping: empty string -> null for complement columns
        mapping_clean = (
            self._mapping_df
            .withColumn("map_cod_mni_complemento",
                when(col("map_cod_mni_complemento").cast("string") == "", None)
                .otherwise(col("map_cod_mni_complemento")))
            .withColumn("map_cod_mni_complemento_tabelado",
                when(col("map_cod_mni_complemento_tabelado").cast("string") == "", None)
                .otherwise(col("map_cod_mni_complemento_tabelado")))
        )

        join_cond = (
            (col("m._mov_int") == col("mp.map_cod_mni_movimento"))
            & (col("mp.map_cod_mni_complemento").isNull()
               | (col("m._comp_int") == col("mp.map_cod_mni_complemento")))
            & (col("mp.map_cod_mni_complemento_tabelado").isNull()
               | (col("m._comp_tab_int") == col("mp.map_cod_mni_complemento_tabelado")))
        )

        df_mapped = (
            df_for_map.alias("m")
            .join(broadcast(mapping_clean).alias("mp"), join_cond, "inner")
            .withColumn("_specificity",
                when(col("mp.map_cod_mni_complemento").isNotNull(), 1).otherwise(0)
                + when(col("mp.map_cod_mni_complemento_tabelado").isNotNull(), 1).otherwise(0))
            .select("m.*", col("mp.map_situacao").alias("situacao"), "_specificity")
        )

        # Keep most specific mapping per event; break ties by smallest situacao code
        w_map = Window.partitionBy("id_processo_evento").orderBy(
            col("_specificity").desc(), col("situacao").asc()
        )
        df_mapped = (
            df_mapped
            .withColumn("_rn", row_number().over(w_map))
            .filter(col("_rn") == 1)
            .drop("_rn", "_specificity", "_mov_int", "_comp_int", "_comp_tab_int")
        )

        # Dedupe: one row per (id_processo, dth_inicio, situacao)
        w_dedup = Window.partitionBy("id_processo", "dth_inicio", "situacao").orderBy(
            col("id_processo_evento").asc_nulls_last()
        )
        df_deduped = (
            df_mapped
            .withColumn("_rn", row_number().over(w_dedup))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # Filter to target month
        df_month = df_deduped.filter(
            (F.year(col("dth_inicio")) == year)
            & (F.month(col("dth_inicio")) == month)
        )

        logger.info("Movements for %d-%02d after mapping — proceeding to write", year, month)
        return df_month

    # ── 3. dth_fim ────────────────────────────────────────────────────────────
    def compute_dth_fim(self, df: DataFrame) -> DataFrame:
        """Apply dth_fim logic derived from situacoes_start_end.json."""
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

        w = Window.partitionBy("id_processo").orderBy("dth_inicio")
        df = (
            df
            .withColumn("_sit_str",  col("situacao").cast("string"))
            .withColumn("_next_sit", lead("_sit_str").over(w))
            .withColumn("_next_dth", lead("dth_inicio").over(w))
        )

        # Instantaneous / self-closing: dth_fim == dth_inicio
        df = df.withColumn("_dth_inst",
            when(col("_sit_str").isin(instantaneous + self_closing), col("dth_inicio")))

        # end_with: broadcast join replaces Python UDF
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
                    when(col("_ew_sit").isNotNull(), col("_next_dth")))
                .drop("_ew_sit", "_ew_nxt")
            )
        else:
            df = df.withColumn("_dth_end", lit(None).cast(TimestampType()))

        # group_end_with: broadcast join replaces Python UDF
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
    def process_special_situacoes(
        self, df: DataFrame, year: int, month: int
    ) -> Optional[DataFrame]:
        """
        Generate situacao 025 and 088 with month-end state logic.

        CHANGE: Removed the redundant date_format filter inside the window's
        when() clause. `df` is already filtered to the target month by
        read_movements(), so filtering by current_month again inside the
        window was a no-op that added unnecessary computation.

        CHANGE: Replaced the unbounded-following F.min() aggregation window
        with a simple lead() — we only need the immediately next closer
        event per processo, not the global minimum across all future rows.
        This avoids an expensive O(n²) shuffle on large partitions.
        """
        logger.info("Processing special situacoes 025 and 088")

        sit_dict = {
            str(s["situacao"]): {
                "start_with": [str(x) for x in s.get("start_with", [])],
                "end_with":   [str(x) for x in s.get("end_with", [])],
            }
            for s in self.situacoes_start_end if "situacao" in s
        }

        results = []

        for sit_key, sit_code in [("25", 25), ("88", 88)]:
            if sit_key not in sit_dict:
                continue
            cfg_sit    = sit_dict[sit_key]
            start_ints = [int(x) for x in cfg_sit["start_with"]]
            end_ints   = [int(x) for x in cfg_sit["end_with"]]
            all_ints   = list(set(start_ints + end_ints))

            # CHANGE: use lead() instead of unbounded-following min() window.
            # Since df is month-scoped, we only look at the next row per processo
            # ordered by dth_inicio to find the immediately following closer event.
            w_ordered = Window.partitionBy("id_processo").orderBy("dth_inicio")

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

    # ── 5. Align schema to DDL ────────────────────────────────────────────────
    def _align_schema(self, df: DataFrame) -> DataFrame:
        """Cast columns to match dw_2inst_ouro.tb_tf_situacao_eproc DDL and add year_month."""
        return (
            df
            .withColumn("id_processo_evento", col("id_processo_evento").cast(StringType()))
            .withColumn("id_processo",         col("id_processo").cast(StringType()))
            .withColumn("num_processo",         col("num_processo").cast(StringType()))
            .withColumn("id_evento_judicial",   col("id_evento_judicial").cast(StringType()))
            .withColumn("cod_mni_complemento",  col("cod_mni_complemento").cast(StringType()))
            .withColumn("cod_mni_complemento_tabelado", col("cod_mni_complemento_tabelado").cast(StringType()))
            .withColumn("year_month",
                when(col("dth_inicio").isNotNull(), date_format(col("dth_inicio"), "yyyy-MM"))
                .otherwise("unknown"))
        )

    # ── 6. Write to Iceberg ───────────────────────────────────────────────────
    def _table_exists(self, full_target: str) -> bool:
        """Reliable existence check via DESCRIBE TABLE (immediate catalog lookup)."""
        try:
            self.spark.sql(f"DESCRIBE TABLE {full_target}")
            return True
        except Exception:
            return False

    def save_to_iceberg(self, df: DataFrame) -> None:
        full_target = tbl(self.cfg.catalog, self.cfg.db_prata, self.cfg.target_table)
        logger.info("Writing to Iceberg: %s", full_target)

        if not self._table_exists(full_target):
            logger.info("Table not found — creating: %s", full_target)
            (
                df.writeTo(full_target)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty("write_compression", "zstd")
                .partitionedBy(months(col("dth_inicio")))
                .create()
            )
        else:
            logger.info("Table exists — overwriting partitions: %s", full_target)
            df.writeTo(full_target).overwritePartitions()

        logger.info("Write completed: %s", full_target)

    # ── 6. Orchestrate ────────────────────────────────────────────────────────
    def run(self, year: int, month: int) -> None:
        logger.info("=" * 70)
        logger.info("eproc_situacao_emr  year=%d  month=%02d", year, month)
        logger.info("=" * 70)

        full_target = tbl(self.cfg.catalog, self.cfg.db_prata, self.cfg.target_table)

        # ── Step 1: main situacoes (dth_inicio=dth_fim or resolved by end_with) ──
        logger.info("Step 1: reading and writing main situacoes")
        df = self.read_movements(year, month)
        df_main = df.filter(~col("situacao").isin([25, 88]))
        df_main = self.compute_dth_fim(df_main)
        df_main = self._align_schema(df_main)

        if self.cfg.dry_run:
            logger.info("DRY RUN — Step 1 schema:")
            df_main.printSchema()
            df_main.groupBy("year_month", "situacao").count().orderBy("year_month", "situacao").show(50, truncate=False)
        else:
            self.save_to_iceberg(df_main)
            logger.info("Step 1 written")

        # ── Step 2: special situacoes 025/088 built from step 1 data ─────────────
        logger.info("Step 2: processing special situacoes 025 and 088")
        df_special = self.process_special_situacoes(df, year, month)

        if df_special is not None:
            df_special = self._align_schema(df_special)
            if self.cfg.dry_run:
                logger.info("DRY RUN — Step 2 schema:")
                df_special.groupBy("year_month", "situacao").count().orderBy("year_month", "situacao").show(50, truncate=False)
            else:
                df_special.writeTo(full_target).append()
                logger.info("Step 2 appended")
        else:
            logger.info("Step 2: no special situacoes generated")

        logger.info("Job completed for %d-%02d", year, month)


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Carga mensal de situação EPROC para Iceberg")
    p.add_argument("--year",  type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--catalog",        default="glue_catalog")
    p.add_argument("--db-eproc",       default="eproc2g")
    p.add_argument("--db-prata",       default="dw_2inst_ouro")
    p.add_argument("--target-table",   default="tb_tf_situacao_eproc")
    p.add_argument("--s3-json-prefix",
                   default="s3://sgdai-projetos/2instancia/2inst_situacao/eproc2g/json",
                   help="S3 prefix where situacoes.json and situacoes_start_end.json live")
    # CHANGE: default warehouse aligned to emr-config.json (was sgdai-projetos)
    p.add_argument("--s3-warehouse",
                   default="s3://sgdai-data-lake/ouro",
                   help="S3 base path for Iceberg warehouse — must match emr-config.json")
    p.add_argument("--driver-memory",             default="8g")
    p.add_argument("--executor-memory",           default="12g")
    p.add_argument("--executor-memory-overhead",  default="2g")
    p.add_argument("--shuffle-partitions", type=int, default=100)
    p.add_argument("--dry-run", default="false")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = JobConfig(
        catalog=args.catalog,
        db_eproc=args.db_eproc,
        db_prata=args.db_prata,
        target_table=args.target_table,
        s3_json_prefix=args.s3_json_prefix,
        s3_warehouse=args.s3_warehouse,
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
        executor_memory_overhead=args.executor_memory_overhead,
        shuffle_partitions=args.shuffle_partitions,
        dry_run=str_to_bool(args.dry_run),
    )

    spark = create_spark_session(cfg)
    try:
        EprocSituacaoJob(spark, cfg).run(args.year, args.month)
    except Exception as exc:
        logger.error("Job falhou: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
