#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_repercussao_emr.py — EJUD Repercussão Geral – monthly load to Iceberg (ouro layer)

Single-script design: no external .py dependencies.
All data reads via spark.table() from the Glue catalog.

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_repercussao/ejud/scripts/ejud_repercussao_emr.py \\
      --year 2026 --month 3 \\
      --executor-memory 12g --executor-memory-overhead 2g 2>&1 | \\
      grep --line-buffered -E 'ejud_repercussao_emr|ERROR|Exception'
"""

import argparse
import json
import logging
import sys
from datetime import date
from typing import List

import boto3
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    broadcast, col, coalesce, concat, current_timestamp,
    lpad, lit, regexp_replace, row_number, to_date, when,
    min as _min, sum as _sum,
)

# ── Constants ─────────────────────────────────────────────────────────────────
_CATALOG            = "glue_catalog"
_WAREHOUSE          = "s3://sgdai-data-lake"
_TARGET_SCHEMA      = "dw_2inst_ouro"
_TARGET_TABLE       = "tb_tf_repercussao"
_PARTITION_COLS     = ["ano_mes", "ind_sistema"]

# Default S3 prefix for JSON configs — overridden by --s3-json-prefix arg
_S3_JSON_PREFIX = "s3://sgdai-projetos/2instancia/2inst_repercussao/ejud/json"


# ── S3 JSON loader ────────────────────────────────────────────────────────────

def _load_json_from_s3(s3_uri: str) -> object:
    without_prefix = s3_uri.removeprefix("s3://")
    bucket, key = without_prefix.split("/", 1)
    logger.info("Reading s3://%s/%s", bucket, key)
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_repercussao_emr")


# ── Iceberg helpers ───────────────────────────────────────────────────────────

def _ensure_iceberg_table(spark, schema: str, table: str, df, partition_cols: List[str]):
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
    non_part = [d for d in col_defs if not any(d.strip().startswith(f"`{p}`") for p in partition_cols)]
    part_def = [d for d in col_defs if any(d.strip().startswith(f"`{p}`") for p in partition_cols)]
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


def _save_to_iceberg(df, schema: str = _TARGET_SCHEMA, table: str = _TARGET_TABLE):
    fqn = f"{_CATALOG}.{schema}.{table}"
    logger.info("Saving to Iceberg: %s", fqn)
    _ensure_iceberg_table(df.sparkSession, schema, table, df, _PARTITION_COLS)
    df.writeTo(fqn).option("write-audit-publish", "false").overwritePartitions()
    logger.info("Iceberg write complete: %s", fqn)


# ── SparkSession ──────────────────────────────────────────────────────────────

def _create_spark_session(
    app_name: str,
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
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
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ── Calculator ────────────────────────────────────────────────────────────────

class RepercussaoCalculator:
    """EJUD Repercussão Geral — ind_sistema=1."""

    def __init__(
        self,
        spark: SparkSession,
        codes: dict,
        catalog: str = _CATALOG,
        db_bronze: str = "ejud",
        db_prata: str = "dw_2inst_prata",
        db_ouro: str = "dw_2inst_ouro",
        db_situacao: str = "dw_2inst_ouro",   # database onde vivem tb_tf_situacao_ejud
    ):
        self.spark       = spark
        self.catalog     = catalog
        self.db_bronze   = db_bronze
        self.db_prata    = db_prata
        self.db_ouro     = db_ouro
        self.db_situacao = db_situacao

        # Loaded from repercussao_codes.json via S3
        self.UNSUSPEND_SITUACAO_CODES = codes["unsuspend_situacao_codes"]
        self.EXCLUDED_COMPETENCIAS    = codes["excluded_competencias"]

    def _tbl(self, db: str, table: str) -> str:
        return f"{self.catalog}.{db}.{table}"

    def _norm(self, column):
        """Strip .0 suffix and scientific notation from numeric-as-string."""
        return regexp_replace(
            regexp_replace(column.cast("string"), r"\.0+$", ""),
            r"^(\d+)\.(\d+)E(\d+)$", "$1$2",
        )

    # ── Data loaders ──────────────────────────────────────────────────────────

    def load_core_repercussao_data(self, dt_ini: date, dt_fim: date) -> DataFrame:
        logger.info("Loading core repercussao data [%s to %s]...", dt_ini, dt_fim)

        df_movsobrestamento = (
            self.spark.table(self._tbl(self.db_bronze, "movsobrestamento"))
            .filter(
                (to_date(col("dthrmov")) <= lit(dt_fim)) &
                col("dthrterminosobre").isNull()
            )
            .select(
                col("coddoc"),
                col("codreper"),
                col("dthrmov"),
                when(
                    regexp_replace(col("codfase").cast("string"), r"\.0$", "")
                    .cast("int").isin(50040, 50201),
                    lit(265)
                ).otherwise(
                    regexp_replace(col("codfase").cast("string"), r"\.0$", "").cast("int")
                ).alias("codfase_int"),
            )
        )

        df_repercussao = (
            self.spark.table(self._tbl(self.db_bronze, "repercussao"))
            .select("codreper", "codtrib", "codsitreper", "dthrjulg",
                    "dtafetacao", "dttransitojulg", "numreper", "descricao",
                    "codtipreper")
        )

        df_tiporeper = (
            self.spark.table(self._tbl(self.db_bronze, "tiporepercussao"))
            .select(
                col("codtipreper").alias("tr_codtipreper"),
                col("descr").alias("tipo_repercussao"),
            )
        )

        df_rpt = (
            self.spark.table(self._tbl(self.db_bronze, "repercussaoparadigmatema"))
            .select("codreper", "codtema", col("codtrib").alias("rpt_codtrib"))
        )

        df_rv = (
            self.spark.table(self._tbl(self.db_bronze, "repercussaovinculada"))
            .select(col("codrepervinc").alias("codreper"))
        )

        df_valid_codreper = (
            df_repercussao.select("codreper")
            .union(df_rpt.select("codreper"))
            .union(df_rv.select("codreper"))
            .distinct()
        )

        df_mov_filtered = (
            df_movsobrestamento.alias("so")
            .join(broadcast(df_valid_codreper).alias("vc"),
                  col("so.codreper") == col("vc.codreper"), "inner")
            .select("so.*")
        )

        df_sit_reper = (
            self.spark.table(self._tbl(self.db_bronze, "situacaorepercussao"))
            .select("codsitreper", col("descricao").alias("situacao_precedente"))
        )

        df_core = (
            df_mov_filtered.alias("so")
            .join(df_repercussao.alias("repe"),
                  col("so.codreper") == col("repe.codreper"), "left")
            .join(broadcast(df_rpt).alias("rpt"),
                  col("so.codreper") == col("rpt.codreper"), "left")
            .join(broadcast(df_sit_reper).alias("sit"),
                  col("repe.codsitreper").cast("long") == col("sit.codsitreper").cast("long"),
                  "left")
            .join(broadcast(df_tiporeper).alias("tiporeper"),
                  col("repe.codtipreper").cast("long") == col("tiporeper.tr_codtipreper").cast("long"),
                  "left")
            .select(
                self._norm(col("so.coddoc")).alias("coddoc"),
                col("so.dthrmov").alias("dt_sobrestamento"),
                col("so.dthrmov").alias("dth_fase"),
                col("so.codfase_int").alias("cod_fase"),
                col("so.codreper").alias("cod_precedente"),
                coalesce(col("rpt.rpt_codtrib").cast("int"),
                         col("repe.codtrib").cast("int")).alias("cod_tribunal"),
                col("repe.dthrjulg").alias("dth_julgamento"),
                col("repe.dtafetacao").alias("dth_afetacao"),
                col("repe.dttransitojulg").alias("dth_transito_julgado"),
                coalesce(col("rpt.codtema"), col("repe.numreper")).alias("num_precedente"),
                col("repe.descricao").alias("questao"),
                col("sit.situacao_precedente"),
                col("tiporeper.tipo_repercussao"),
            )
            .filter(col("cod_tribunal").isin(13, 91, 92))
        )

        logger.info("Core data loaded")
        return df_core

    def filter_active_suspensions(self, df_core: DataFrame) -> DataFrame:
        """Remove suspensions lifted via dw_2inst_ouro.tb_tf_situacao_ejud."""
        logger.info("Filtering active suspensions via dw_2inst_ouro.tb_tf_situacao_ejud...")
        try:
            df_unsuspend = (
                self.spark.table(self._tbl(self.db_situacao, "tb_tf_situacao_ejud"))
                .withColumn("coddoc", self._norm(col("coddoc")))
                .filter(col("situacao").isin(self.UNSUSPEND_SITUACAO_CODES))
                .select("coddoc", col("dthrmov").alias("dth_unsuspend"))
            )

            df_active = (
                df_core.alias("c")
                .join(
                    df_unsuspend.alias("u"),
                    (col("c.coddoc") == col("u.coddoc")) &
                    (col("u.dth_unsuspend") > col("c.dt_sobrestamento")),
                    "left_anti",
                )
            )

            before = df_core.count()
            after  = df_active.count()
            logger.info("Active suspensions: %d → %d (%d lifted removed)", before, after, before - after)
            return df_active

        except Exception as e:
            logger.warning("Could not filter active suspensions: %s — continuing without filter", e)
            return df_core

    def filter_concluded_movements(self, df_core: DataFrame) -> DataFrame:
        """Remove suspensions concluded via movimento (codfase=50002, codcompl=50118)."""
        logger.info("Filtering concluded movements...")
        try:
            min_sob = df_core.select(_min(col("dt_sobrestamento")).alias("m")).first()["m"]
            if min_sob is None:
                return df_core
            min_sob_date = min_sob if isinstance(min_sob, date) else min_sob.date()
            min_year, min_month = min_sob_date.year, min_sob_date.month

            df_coddocs = broadcast(df_core.select("coddoc").distinct())

            df_concluded = (
                self.spark.table(self._tbl(self.db_bronze, "movimento"))
                .filter(
                    (col("ano") > lit(min_year)) |
                    ((col("ano") == lit(min_year)) & (col("mes") >= lit(min_month)))
                )
                .withColumn("coddoc", self._norm(col("coddoc")))
                .join(df_coddocs, on="coddoc", how="inner")
                .withColumn("codfase_c",
                            regexp_replace(col("codfase").cast("string"), r"\.0$", "").cast("int"))
                .withColumn("codcompl1_c",
                            regexp_replace(col("codcompl1").cast("string"), r"\.0$", "").cast("int"))
                .withColumn("codcompl2_c",
                            regexp_replace(col("codcompl2").cast("string"), r"\.0$", "").cast("int"))
                .filter(
                    (col("codfase_c") == 50002) &
                    ((col("codcompl1_c") == 50118) | (col("codcompl2_c") == 50118))
                )
                .select("coddoc", col("dthrmov").alias("dth_concluded"))
            )

            df_active = (
                df_core.alias("c")
                .join(
                    df_concluded.alias("m"),
                    (col("c.coddoc") == col("m.coddoc")) &
                    (col("m.dth_concluded") > col("c.dt_sobrestamento")),
                    "left_anti",
                )
            )

            before = df_core.count()
            after  = df_active.count()
            logger.info("Concluded movements: %d → %d (%d removed)", before, after, before - after)
            return df_active

        except Exception as e:
            logger.warning("Could not filter concluded movements: %s — continuing", e)
            return df_core

    def deduplicate(self, df: DataFrame) -> DataFrame:
        logger.info("Deduplicating by (coddoc, cod_precedente, cod_tribunal)...")
        w = Window.partitionBy("coddoc", "cod_precedente", "cod_tribunal").orderBy("dt_sobrestamento")
        return (
            df.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn")
        )

    def load_processo(self, df_coddoc_filter: DataFrame) -> DataFrame:
        logger.info("Loading t_processo...")

        w_assunto = Window.partitionBy("coddoc").orderBy(col("numord").asc())
        df_assunto = (
            self.spark.table(self._tbl(self.db_bronze, "processoassunto"))
            .filter(col("numord") == 1)
            .withColumn("_rn", row_number().over(w_assunto))
            .filter(col("_rn") == 1)
            .select("coddoc", col("codass").cast("string").alias("cod_assunto_princ"))
        )

        df_proc = (
            self.spark.table(self._tbl(self.db_bronze, "t_processo"))
            .join(broadcast(df_coddoc_filter), on="coddoc", how="inner")
            .filter(~col("codcompt").isin(self.EXCLUDED_COMPETENCIAS))
            .withColumn("coddoc", self._norm(col("coddoc")))
            .select(
                col("coddoc"),
                col("numsequencial"), col("dv"), col("numano"),
                col("codjustica"), col("codregi"), col("codlocregi"),
                col("codtipproc").cast("string").alias("cod_classe"),
                col("indsupersigil"), col("indsigil"), col("indsegrjust"),
            )
            .withColumn("processo", concat(
                lpad(col("numsequencial").cast("string"), 7, "0"),
                lpad(col("dv").cast("string"), 2, "0"),
                col("numano").cast("string"),
                col("codjustica").cast("string"),
                col("codregi").cast("string"),
                lpad(col("codlocregi").cast("string"), 4, "0"),
            ))
            .withColumn("cod_sigilo",
                when(col("indsupersigil") == "S", "5")
                .when(col("indsigil") == "S", "4")
                .when(col("indsegrjust") == "S", "1")
                .otherwise("0"))
            .withColumn("ind_originario",
                when(col("processo").endswith("0000"), "S").otherwise("N"))
            .join(broadcast(df_assunto), on="coddoc", how="left")
            .select("coddoc", "processo", "cod_classe", "cod_assunto_princ",
                    "cod_sigilo", "ind_originario")
        )

        logger.info("t_processo loaded")
        return df_proc

    def enrich_with_processo(self, df_core: DataFrame) -> DataFrame:
        logger.info("Enriching with processo...")
        df_proc = self.load_processo(df_core.select("coddoc").distinct())
        return (
            df_core.alias("rep")
            .join(broadcast(df_proc).alias("proc"),
                  col("rep.coddoc") == col("proc.coddoc"), "inner")
            .select(
                col("rep.*"),
                col("proc.processo"),
                col("proc.cod_classe"),
                col("proc.cod_assunto_princ"),
                col("proc.cod_sigilo"),
                col("proc.ind_originario"),
            )
        )

    def load_movimentorelator(self, df_coddoc_filter: DataFrame) -> DataFrame:
        logger.info("Loading movimentorelator...")
        return (
            self.spark.table(self._tbl(self.db_bronze, "movimentorelator"))
            .withColumn("coddoc", self._norm(col("coddoc")))
            .join(broadcast(df_coddoc_filter), on="coddoc", how="inner")
            .select(
                "coddoc", "dthrmov",
                col("codorgjulg").cast("string").alias("cod_orgao_julgador"),
                col("codmagrel").cast("string").alias("cod_relator"),
            )
        )

    def calculate_last_relator(self, df_mov_relator: DataFrame) -> DataFrame:
        logger.info("Calculating last relator...")
        w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        return (
            df_mov_relator
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("coddoc",
                    col("cod_orgao_julgador").alias("cod_oj"),
                    "cod_relator")
        )

    def load_ejud_situacao(self, df_coddoc_filter: DataFrame) -> DataFrame:
        """Last movement per coddoc from dw_2inst_ouro.tb_tf_situacao_ejud."""
        logger.info("Loading dw_2inst_ouro.tb_tf_situacao_ejud...")
        try:
            w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
            return (
                self.spark.table(self._tbl(self.db_situacao, "tb_tf_situacao_ejud"))
                .withColumn("coddoc", self._norm(col("coddoc")))
                .join(broadcast(df_coddoc_filter), on="coddoc", how="inner")
                .withColumn("rn", row_number().over(w))
                .filter(col("rn") == 1)
                .select(
                    "coddoc",
                    col("dthrmov").alias("dth_fase"),
                    col("cod_tip_mov_numero").cast("int").alias("cod_fase"),
                )
            )
        except Exception as e:
            logger.warning("Could not load ejud_situacao: %s — using NULL cod_fase", e)
            return self.spark.createDataFrame(
                [], "coddoc STRING, dth_fase TIMESTAMP, cod_fase INT"
            )

    def load_last_movimento_ejud(self, df_coddoc_filter: DataFrame) -> DataFrame:
        """Last movement code/date for cod_ult_movimento / dth_ult_movimento."""
        logger.info("Loading last movimento from dw_2inst_ouro.tb_tf_situacao_ejud...")
        try:
            w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
            return (
                self.spark.table(self._tbl(self.db_situacao, "tb_tf_situacao_ejud"))
                .withColumn("coddoc", self._norm(col("coddoc")))
                .join(broadcast(df_coddoc_filter), on="coddoc", how="inner")
                .withColumn("rn", row_number().over(w))
                .filter(col("rn") == 1)
                .select(
                    "coddoc",
                    col("cod_tip_mov_numero").cast("string").alias("cod_ult_movimento"),
                    col("dthrmov").alias("dth_ult_movimento"),
                )
            )
        except Exception as e:
            logger.warning("Could not load last movimento: %s", e)
            return self.spark.createDataFrame(
                [], "coddoc STRING, cod_ult_movimento STRING, dth_ult_movimento TIMESTAMP"
            )

    def load_movimentofisico(self, df_coddoc_filter: DataFrame) -> DataFrame:
        """Last physical location from movimentofisico (bronze)."""
        logger.info("Loading last local from movimentofisico...")
        try:
            w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
            return (
                self.spark.table(self._tbl(self.db_bronze, "movimentofisico"))
                .withColumn("coddoc", self._norm(col("coddoc")))
                .join(broadcast(df_coddoc_filter), on="coddoc", how="inner")
                .withColumn("rn", row_number().over(w))
                .filter(col("rn") == 1)
                .select(
                    "coddoc",
                    regexp_replace(col("codlocal").cast("string"), r"\.0$", "").alias("cod_local"),
                )
            )
        except Exception as e:
            logger.warning("Could not load movimentofisico: %s", e)
            return self.spark.createDataFrame([], "coddoc STRING, cod_local STRING")

    def load_ejud_acervo(self, df_coddoc_filter: DataFrame, dt_ref: date) -> DataFrame:
        today = date.today()
        is_current = (dt_ref.year == today.year and dt_ref.month == today.month)
        mode = "DIA_ATUAL" if is_current else f"historico {dt_ref.year}-{dt_ref.month:02d}"
        logger.info("Loading ejud_acervo | dt_ref=%s | mode=%s", dt_ref, mode)
        try:
            tbl = self.spark.table(self._tbl(self.db_ouro, "tb_ft_acervo_ejud"))
            if is_current:
                df_base = tbl.filter(col("tipo") == "DIA_ATUAL")
            else:
                df_base = tbl.filter(
                    (col("tipo") == "ULTIMO_DIA_MES") &
                    (col("ano") == dt_ref.year) &
                    (col("mes") == dt_ref.month)
                )
            return (
                df_base
                .withColumn("coddoc", self._norm(col("coddoc")))
                .join(broadcast(df_coddoc_filter), on="coddoc", how="inner")
                .select("coddoc", lit(True).alias("in_acervo"))
                .dropDuplicates(["coddoc"])
            )
        except Exception as e:
            logger.warning("Could not load ejud_acervo: %s — ind_acervo=False", e)
            return self.spark.createDataFrame([], "coddoc STRING, in_acervo BOOLEAN")

    # ── Diagnostics ──────────────────────────────────────────────────────────

    def log_null_counts(self, df: DataFrame, context: str = "") -> None:
        MUST_NOT_BE_NULL = {
            "cod_documento", "processo", "ind_sistema",
            "cod_precedente", "num_precedente",
            "cod_tribunal", "questao", "dth_admissao", "ano_mes",
        }
        total = df.count()
        if total == 0:
            logger.warning("[null-check %s] DataFrame is empty", context)
            return

        null_counts = df.select([
            _sum(when(col(c).isNull() | (col(c).cast("string") == ""), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()

        logger.info("[null-check %s] total=%d", context, total)
        for column, nulls in null_counts.items():
            if nulls == 0:
                continue
            pct = nulls * 100 / total
            msg = f"  {column}: {nulls}/{total} ({pct:.1f}%)"
            if column in MUST_NOT_BE_NULL:
                logger.warning("⚠  %s  <-- SHOULD NOT BE NULL", msg)
            else:
                logger.info(msg)

    # ── Final fact table ─────────────────────────────────────────────────────

    def build_final_fact_table(self, df: DataFrame, dt_ref: date) -> DataFrame:
        logger.info("Building final fact table...")
        return df.select(
            self._norm(col("coddoc")).alias("cod_documento"),
            col("processo"),
            lit(1).alias("ind_sistema"),
            col("cod_classe"),
            col("cod_assunto_princ"),
            col("cod_sigilo"),
            col("cod_relator"),
            col("cod_relator").alias("cod_mag"),
            col("cod_oj"),
            col("cod_fase"),
            col("dth_fase"),
            col("ind_originario"),
            current_timestamp().alias("dth_referencia"),
            col("cod_tribunal"),
            when(col("cod_tribunal") == 91, "Supremo Tribunal Federal - STF")
            .when(col("cod_tribunal") == 92, "Superior Tribunal de Justiça - STJ")
            .when(col("cod_tribunal") == 13, "Tribunal de Justiça - RJ")
            .otherwise(lit(None).cast("string")).alias("tribunal"),
            col("cod_precedente").cast("string"),
            col("questao").alias("precedente"),
            col("num_precedente").cast("string"),
            col("questao"),
            col("dth_afetacao"),
            col("dt_sobrestamento").alias("dth_admissao"),
            col("dth_julgamento"),
            col("dth_transito_julgado"),
            col("situacao_precedente"),
            col("tipo_repercussao"),
            when(col("in_acervo").isNotNull(), lit(True)).otherwise(lit(False)).alias("ind_acervo"),
            col("cod_ult_movimento"),
            col("dth_ult_movimento"),
            col("cod_local"),
            lit(dt_ref.strftime("%Y_%m")).alias("ano_mes"),
        )

    # ── Main calculation ──────────────────────────────────────────────────────

    def calculate(self, dt_ini: date, dt_fim: date, dt_ref: date) -> DataFrame:
        logger.info("=" * 70)
        logger.info("Starting EJUD Repercussao | %s → %s | ref=%s", dt_ini, dt_fim, dt_ref)
        logger.info("=" * 70)

        # 1. Core data
        # dthrterminosobre IS NULL filter already applied in load_core_repercussao_data —
        # that is the sole EJUD criterion for active sobrestamento.
        df_core = self.load_core_repercussao_data(dt_ini, dt_fim)

        # 2. Deduplicate
        df_core = self.deduplicate(df_core)

        # 4. Enrich with processo
        df = self.enrich_with_processo(df_core)

        # 5. Cache coddoc list for downstream joins
        df_coddocs = df.select("coddoc").distinct().cache()
        df_coddocs.count()

        try:
            # 6. Last relator / orgao
            mov_relator  = self.load_movimentorelator(df_coddocs)
            last_relator = self.calculate_last_relator(mov_relator)
            df = df.join(broadcast(last_relator), on="coddoc", how="left")

            # 7. Acervo flag
            df_acervo = self.load_ejud_acervo(df_coddocs, dt_ref)
            df = df.join(broadcast(df_acervo), on="coddoc", how="left")

            # 8. Last movement (cod_ult_movimento / dth_ult_movimento)
            df_last_mov = self.load_last_movimento_ejud(df_coddocs)
            df = df.join(broadcast(df_last_mov), on="coddoc", how="left")

            # 9. Last physical location (cod_local)
            df_local = self.load_movimentofisico(df_coddocs)
            df = df.join(broadcast(df_local), on="coddoc", how="left")

            # 10. Build final fact table
            df_final = self.build_final_fact_table(df, dt_ref)

            # Guard: rows without a precedente violate PK
            df_final = df_final.filter(
                col("cod_precedente").isNotNull() & (col("cod_precedente") != "")
            )

            # Safety dedup on PK
            pk_cols = ["cod_documento", "cod_precedente", "cod_tribunal", "ano_mes"]
            w_pk = Window.partitionBy(*pk_cols).orderBy(col("dth_admissao").asc())
            df_final = (
                df_final
                .withColumn("_rn_pk", row_number().over(w_pk))
                .filter(col("_rn_pk") == 1)
                .drop("_rn_pk")
            )

            # Cache before diagnostics — df_final is lazy (un-cached) at this point.
            # Without cache, log_null_counts triggers two full re-executions of the entire
            # pipeline (df.count() + collect()), each re-running the 125-task PK-dedup
            # shuffle and re-broadcasting all 4 lookup tables.
            df_final.cache()
            n_final = df_final.count()   # single materialisation — reused by both log and caller
            self.log_null_counts(df_final, context="ejud final")
            logger.info("Final result: %d rows", n_final)

        finally:
            df_coddocs.unpersist()

        return df_final

    def save(
        self,
        df: DataFrame,
        schema: str = _TARGET_SCHEMA,
        table: str = _TARGET_TABLE,
        ano_mes: str = None,
    ):
        logger.info("Saving ind_sistema=1 | ano_mes=%s → %s.%s", ano_mes, schema, table)
        _save_to_iceberg(df, schema, table)


# ── Month helpers ─────────────────────────────────────────────────────────────

def resolve_month(year_arg, month_arg):
    """Default to current month; on day 1 use previous month."""
    if year_arg and month_arg:
        return year_arg, month_arg
    today = date.today()
    year, month = today.year, today.month
    if today.day == 1:
        d = date(year, month, 1) - relativedelta(months=1)
        year, month = d.year, d.month
    return year, month


def month_bounds(year: int, month: int):
    dt_ini = date(year, month, 1)
    dt_fim = dt_ini + relativedelta(months=1) - relativedelta(days=1)
    return dt_ini, dt_fim, dt_fim


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="EJUD Repercussão – monthly load to Iceberg (ouro)")
    p.add_argument("--year",  type=int, help="Year  (e.g. 2026)")
    p.add_argument("--month", type=int, help="Month (1-12)")
    p.add_argument("--catalog",          default="glue_catalog")
    p.add_argument("--db-bronze",        default="ejud",  dest="db_bronze")
    p.add_argument("--db-prata",         default="dw_2inst_prata", dest="db_prata")
    p.add_argument("--db-ouro",          default="dw_2inst_ouro", dest="db_ouro")
    p.add_argument("--db-situacao",      default="dw_2inst_ouro", dest="db_situacao",
                   help="Database da tb_tf_situacao_ejud (onde vive o histórico)")
    p.add_argument("--target-db",        default=_TARGET_SCHEMA, dest="target_db")
    p.add_argument("--target-table",     default=_TARGET_TABLE,  dest="target_table")
    p.add_argument("--s3-json-prefix",   default=_S3_JSON_PREFIX, dest="s3_json_prefix",
                   help="S3 prefix where repercussao_codes.json lives")
    p.add_argument("--executor-memory",          default="12g",  dest="executor_memory")
    p.add_argument("--executor-memory-overhead", default="2g",   dest="executor_memory_overhead")
    p.add_argument("--shuffle-partitions",       default=200,    type=int, dest="shuffle_partitions")
    return p.parse_args()


def main():
    args = parse_args()
    year, month = resolve_month(args.year, args.month)
    dt_ini, dt_fim, dt_ref = month_bounds(year, month)
    ano_mes = dt_ref.strftime("%Y_%m")

    logger.info("=" * 70)
    logger.info("EJUD Repercussão | %s → %s | ano_mes=%s", dt_ini, dt_fim, ano_mes)
    logger.info("target: %s.%s", args.target_db, args.target_table)
    logger.info("=" * 70)

    # Load codes from S3 JSON before starting Spark
    s3p = args.s3_json_prefix.rstrip("/")
    codes = _load_json_from_s3(f"{s3p}/repercussao_codes.json")
    logger.info("Loaded repercussao_codes.json: %d unsuspend codes, %d excluded competencias",
                len(codes["unsuspend_situacao_codes"]), len(codes["excluded_competencias"]))

    spark = _create_spark_session(
        app_name=f"EJUD_Repercussao_{ano_mes}",
        executor_memory=args.executor_memory,
        executor_memory_overhead=args.executor_memory_overhead,
        shuffle_partitions=args.shuffle_partitions,
    )
    logger.info("Spark %s ready", spark.version)

    try:
        calc = RepercussaoCalculator(
            spark,
            codes=codes,
            catalog=args.catalog,
            db_bronze=args.db_bronze,
            db_prata=args.db_prata,
            db_ouro=args.db_ouro,
            db_situacao=args.db_situacao,
        )
        df = calc.calculate(dt_ini, dt_fim, dt_ref)
        df.persist()
        calc.save(df, schema=args.target_db, table=args.target_table, ano_mes=ano_mes)
        logger.info("Done – %d rows written | ano_mes=%s", df.count(), ano_mes)
        df.unpersist()
    except Exception as exc:
        logger.error("Job failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
