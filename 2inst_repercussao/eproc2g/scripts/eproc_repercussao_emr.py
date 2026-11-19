#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eproc_repercussao_emr.py — eProc2g Repercussão Geral – monthly load to Iceberg (ouro layer)

Single-script design: no external .py dependencies.
All data reads via spark.table() from the Glue catalog.

Usage:
    spark-submit --master yarn --deploy-mode client \
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
      s3://sgdai-projetos/2instancia/2inst_repercussao/eproc2g/scripts/eproc_repercussao_emr.py \
      --year 2026 --month 3 \
      --executor-memory 12g --executor-memory-overhead 2g 2>&1 | \
      grep --line-buffered -E 'eproc_repercussao_emr|ERROR|Exception'
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
    broadcast, col, current_timestamp,
    lpad, lit, regexp_extract, regexp_replace, row_number, to_date, when,
    sum as _sum,
)

# ── Constants ─────────────────────────────────────────────────────────────────
_CATALOG            = "glue_catalog"
_WAREHOUSE          = "s3://sgdai-data-lake"
_TARGET_SCHEMA      = "dw_2inst_ouro"
_TARGET_TABLE       = "tb_tf_repercussao"
_PARTITION_COLS     = ["ano_mes", "ind_sistema"]

# Default S3 prefix for JSON configs — overridden by --s3-json-prefix arg
_S3_JSON_PREFIX = "s3://sgdai-projetos/2instancia/2inst_repercussao/eproc2g/json"


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
logger = logging.getLogger("eproc_repercussao_emr")


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

class EprocRepercussaoCalculator:
    """eProc2g Repercussão Geral — ind_sistema=2."""

    def __init__(
        self,
        spark: SparkSession,
        codes: dict,
        catalog: str = _CATALOG,
        db_bronze: str = "eproc2g",
        db_prata: str = "dw_2inst_prata",
        db_ouro: str = "dw_2inst_ouro",       # database onde vivem tb_ft_acervo_eproc, etc.
        db_situacao: str = "dw_2inst_ouro",   # database onde vivem tb_tf_situacao_eproc
    ):
        self.spark       = spark
        self.catalog     = catalog
        self.db_bronze   = db_bronze
        self.db_prata    = db_prata
        self.db_ouro     = db_ouro
        self.db_situacao = db_situacao

        # Loaded from repercussao_codes.json via S3
        self.SUSPEND_CODES            = codes["suspend_codes"]
        self.UNSUSPEND_CODES          = codes["unsuspend_codes"]
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

        # evento_judicial filtered to suspend codes (broadcast — small table)
        df_evento = (
            self.spark.table(self._tbl(self.db_bronze, "evento_judicial"))
            .filter(col("cod_mni_movimento").isin(self.SUSPEND_CODES))
            .select(
                col("id_evento_judicial"),
                regexp_replace(col("cod_mni_movimento").cast("string"), r"\.0$", "")
                .cast("int").alias("cod_fase"),
            )
        )

        # processo_evento joined with evento_judicial so cod_fase is in df_pe
        df_pe = (
            self.spark.table(self._tbl(self.db_bronze, "processo_evento"))
            .join(broadcast(df_evento), on="id_evento_judicial", how="inner")
            .filter(col("dth_desativacao").isNull())
            .filter(to_date(col("dth_evento")) <= lit(dt_fim))
            .select(
                col("id_processo"),
                col("dth_evento"),
                col("dth_evento").alias("dth_fase"),
                col("cod_fase"),
            )
        )

        df_ppj = (
            self.spark.table(self._tbl(self.db_bronze, "processo_paradigma_judicial"))
            .filter(col("dth_desativacao").isNull())
            .select("id_processo", "cod_paradigma_judicial")
        )

        df_pj = (
            self.spark.table(self._tbl(self.db_bronze, "paradigma_judicial"))
            .filter(col("dth_desativacao").isNull())
            .select(
                "cod_paradigma_judicial", "cod_tipo_processo_paradigma",
                "id_situacao_paradigma", "dta_afetacao", "dta_transito_julgado",
                "dta_julgamento", "dta_suspensao", "txt_complemento_assunto",
            )
        )

        df_tpp = (
            self.spark.table(self._tbl(self.db_bronze, "tipo_processo_paradigma"))
            .select("cod_tipo_processo_paradigma", "sig_orgao_cnj",
                    "des_tipo_processo_paradigma")
        )

        df_sp = (
            self.spark.table(self._tbl(self.db_bronze, "situacao_paradigma"))
            .select("id_situacao_paradigma", "des_situacao_paradigma")
        )

        df_core = (
            df_pe.alias("pe")
            .join(df_ppj.alias("ppj"),
                  col("pe.id_processo") == col("ppj.id_processo"), "inner")
            .join(broadcast(df_pj).alias("pj"),
                  col("ppj.cod_paradigma_judicial") == col("pj.cod_paradigma_judicial"), "inner")
            .join(broadcast(df_tpp).alias("tpp"),
                  col("pj.cod_tipo_processo_paradigma") == col("tpp.cod_tipo_processo_paradigma"),
                  "left")
            .join(broadcast(df_sp).alias("sp"),
                  col("pj.id_situacao_paradigma") == col("sp.id_situacao_paradigma"), "left")
            .select(
                self._norm(col("pe.id_processo")).alias("id_processo"),
                col("pe.dth_evento").alias("dt_sobrestamento"),
                col("pe.dth_fase"),
                col("pe.cod_fase"),
                col("pj.cod_paradigma_judicial").alias("cod_precedente"),
                col("pj.dta_afetacao").alias("dth_afetacao"),
                col("pj.dta_transito_julgado").alias("dth_transito_julgado"),
                col("pj.dta_julgamento").alias("dth_julgamento"),
                col("pj.txt_complemento_assunto").alias("questao"),
                when(col("tpp.sig_orgao_cnj") == "STF", lit(91))
                .when(col("tpp.sig_orgao_cnj") == "STJ", lit(92))
                .when(col("tpp.sig_orgao_cnj").isNull(), lit(13))
                .otherwise(lit(None).cast("int")).alias("cod_tribunal"),
                col("sp.des_situacao_paradigma").alias("situacao_precedente"),
                regexp_extract(
                    col("tpp.des_tipo_processo_paradigma"), r"^([^ /]+)", 1
                ).alias("tipo_repercussao"),
            )
        )

        logger.info("Core data loaded")
        return df_core

    def filter_active_suspensions(self, df_core: DataFrame) -> DataFrame:
        """Remove suspensions lifted via processo_evento unsuspend codes."""
        logger.info("Filtering active suspensions...")

        df_evento_un = (
            self.spark.table(self._tbl(self.db_bronze, "evento_judicial"))
            .filter(col("cod_mni_movimento").isin(self.UNSUSPEND_CODES))
            .select("id_evento_judicial")
        )

        df_pe_un = (
            self.spark.table(self._tbl(self.db_bronze, "processo_evento"))
            .join(broadcast(df_evento_un), on="id_evento_judicial", how="inner")
            .filter(col("dth_desativacao").isNull())
            .select(
                self._norm(col("id_processo")).alias("id_processo"),
                col("dth_evento").alias("dth_unsuspend"),
            )
        )

        df_active = (
            df_core.alias("c")
            .join(
                df_pe_un.alias("u"),
                (col("c.id_processo") == col("u.id_processo")) &
                (col("u.dth_unsuspend") > col("c.dt_sobrestamento")),
                "left_anti",
            )
        )

        logger.info("Active suspensions filtered")
        return df_active

    def deduplicate(self, df: DataFrame) -> DataFrame:
        logger.info("Deduplicating by (id_processo, cod_precedente, cod_tribunal)...")
        w = Window.partitionBy("id_processo", "cod_precedente", "cod_tribunal").orderBy(
            col("dt_sobrestamento").asc(),
            when(col("cod_fase") == 265, 0).otherwise(1).asc(),
        )
        return (
            df.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn")
        )

    def load_processo(self, df_id_filter: DataFrame) -> DataFrame:
        logger.info("Loading eproc2g processo...")

        df_rcjm = (
            self.spark.table(self._tbl(self.db_bronze, "relacao_classe_judicial_mni"))
            .select("id_classe_judicial", "cod_mni_classe_judicial")
        )
        df_cj = (
            self.spark.table(self._tbl(self.db_bronze, "classe_judicial"))
            .select("id_classe_judicial", "sin_originario")
        )
        df_aj = (
            self.spark.table(self._tbl(self.db_bronze, "assunto_judicial"))
            .select("id_assunto_judicial", "cod_mni_assunto_judicial")
        )

        return (
            self.spark.table(self._tbl(self.db_bronze, "processo"))
            .withColumn("id_processo", self._norm(col("id_processo")))
            .join(broadcast(df_id_filter), on="id_processo", how="inner")
            .join(broadcast(df_rcjm), on="id_classe_judicial", how="left")
            .join(broadcast(df_cj),   on="id_classe_judicial", how="left")
            .join(broadcast(df_aj),
                  col("id_assunto_principal") == col("id_assunto_judicial"), "left")
            .filter(~col("cod_competencia").isin(self.EXCLUDED_COMPETENCIAS))
            .select(
                col("id_processo"),
                lpad(col("num_processo").cast("string"), 20, "0").alias("processo"),
                col("cod_mni_classe_judicial").cast("string").alias("cod_classe"),
                regexp_replace(col("cod_mni_assunto_judicial").cast("string"), r"\.0$", "")
                .alias("cod_assunto_princ"),
                col("id_orgao_colegiado"),
                col("id_sigilo").cast("string").alias("cod_sigilo"),
                when(col("sin_originario") == "S", "S").otherwise("N").alias("ind_originario"),
                regexp_replace(col("id_orgao_secretaria").cast("string"), r"\.0$", "")
                .alias("cod_local"),
            )
        )

    def load_historico_distribuicao(self, df_id_filter: DataFrame) -> DataFrame:
        logger.info("Loading historico_distribuicao_judicial...")
        return (
            self.spark.table(self._tbl(self.db_bronze, "historico_distribuicao_judicial"))
            .withColumn("id_processo", self._norm(col("id_processo")))
            .join(broadcast(df_id_filter), on="id_processo", how="inner")
            .select("id_processo", "dth_distribuicao", "cod_magistrado")
        )

    def calculate_last_relator(self, df_hdj: DataFrame) -> DataFrame:
        logger.info("Calculating last relator...")
        w = Window.partitionBy("id_processo").orderBy(col("dth_distribuicao").desc())
        return (
            df_hdj
            .filter(col("cod_magistrado").isNotNull())
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("id_processo", col("cod_magistrado").cast("string").alias("cod_relator"))
        )

    def load_eproc2g_situacao(self, df_id_filter: DataFrame) -> DataFrame:
        """Last movement per id_processo — cod_fase / dth_fase."""
        logger.info("Loading dw_2inst_ouro.tb_tf_situacao_eproc...")
        try:
            w = Window.partitionBy("id_processo").orderBy(col("dth_inicio").desc())
            return (
                self.spark.table(self._tbl(self.db_situacao, "tb_tf_situacao_eproc"))
                .withColumn("id_processo", self._norm(col("id_processo")))
                .join(broadcast(df_id_filter), on="id_processo", how="inner")
                .withColumn("rn", row_number().over(w))
                .filter(col("rn") == 1)
                .select(
                    "id_processo",
                    col("dth_inicio").alias("dth_fase"),
                    regexp_replace(col("cod_mni_movimento").cast("string"), r"\.0$", "")
                    .cast("int").alias("cod_fase"),
                )
            )
        except Exception as e:
            logger.warning("Could not load eproc2g situacao: %s — using NULL cod_fase", e)
            return self.spark.createDataFrame(
                [], "id_processo STRING, dth_fase TIMESTAMP, cod_fase INT"
            )

    def load_eproc2g_acervo(self, df_id_filter: DataFrame, dt_ref: date) -> DataFrame:
        today = date.today()
        is_current = (dt_ref.year == today.year and dt_ref.month == today.month)
        mode = "DIA_ATUAL" if is_current else f"historico {dt_ref.year}-{dt_ref.month:02d}"
        logger.info("Loading eproc2g_acervo | dt_ref=%s | mode=%s", dt_ref, mode)
        try:
            tbl = self.spark.table(self._tbl(self.db_ouro, "tb_ft_acervo_eproc"))
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
                .withColumn("id_processo", self._norm(col("id_processo")))
                .join(broadcast(df_id_filter), on="id_processo", how="inner")
                .select("id_processo", lit(True).alias("in_acervo"))
                .dropDuplicates(["id_processo"])
            )
        except Exception as e:
            logger.warning("Could not load eproc2g_acervo: %s — ind_acervo=False", e)
            return self.spark.createDataFrame([], "id_processo STRING, in_acervo BOOLEAN")

    def load_last_movimento_eproc(self, df_id_filter: DataFrame) -> DataFrame:
        """Last movement code/date for cod_ult_movimento / dth_ult_movimento."""
        logger.info("Loading last movimento from dw_2inst_ouro.tb_tf_situacao_eproc...")
        try:
            w = Window.partitionBy("id_processo").orderBy(col("dth_inicio").desc())
            return (
                self.spark.table(self._tbl(self.db_situacao, "tb_tf_situacao_eproc"))
                .withColumn("id_processo", self._norm(col("id_processo")))
                .join(broadcast(df_id_filter), on="id_processo", how="inner")
                .withColumn("rn", row_number().over(w))
                .filter(col("rn") == 1)
                .select(
                    "id_processo",
                    col("cod_mni_movimento").cast("string").alias("cod_ult_movimento"),
                    col("dth_inicio").alias("dth_ult_movimento"),
                )
            )
        except Exception as e:
            logger.warning("Could not load last movimento: %s", e)
            return self.spark.createDataFrame(
                [], "id_processo STRING, cod_ult_movimento STRING, dth_ult_movimento TIMESTAMP"
            )

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
            col("id_processo").alias("cod_documento"),
            col("processo"),
            lit(2).alias("ind_sistema"),
            col("cod_classe"),
            col("cod_assunto_princ"),
            col("cod_sigilo"),
            self._norm(col("cod_relator")).alias("cod_relator"),
            self._norm(col("cod_relator")).alias("cod_mag"),
            self._norm(col("id_orgao_colegiado")).alias("cod_oj"),
            col("cod_fase"),
            col("dth_fase"),
            col("ind_originario"),
            current_timestamp().alias("dth_referencia"),
            col("cod_tribunal"),
            when(col("cod_tribunal") == 91, "Supremo Tribunal Federal - STF")
            .when(col("cod_tribunal") == 92, "Superior Tribunal de Justiça - STJ")
            .when(col("cod_tribunal") == 13, "Tribunal de Justiça - RJ")
            .otherwise(lit(None).cast("string")).alias("tribunal"),
            self._norm(col("cod_precedente")).alias("cod_precedente"),
            col("questao").alias("precedente"),
            self._norm(col("cod_precedente")).alias("num_precedente"),
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
        logger.info("Starting eProc Repercussao | %s → %s | ref=%s", dt_ini, dt_fim, dt_ref)
        logger.info("=" * 70)

        # 1. Core data
        df_core = self.load_core_repercussao_data(dt_ini, dt_fim)

        # 2. Remove lifted suspensions
        df_core = self.filter_active_suspensions(df_core)

        # 3. Deduplicate
        df_core = self.deduplicate(df_core)

        # 4. Enrich with processo
        df_id_processos = df_core.select("id_processo").distinct()
        df_proc = self.load_processo(df_id_processos)

        df = (
            df_core.alias("rep")
            .join(broadcast(df_proc).alias("proc"),
                  col("rep.id_processo") == col("proc.id_processo"), "inner")
            .select(
                col("rep.*"),
                col("proc.processo"),
                col("proc.cod_classe"),
                col("proc.cod_assunto_princ"),
                col("proc.id_orgao_colegiado"),
                col("proc.cod_sigilo"),
                col("proc.ind_originario"),
                col("proc.cod_local"),
            )
        )

        # 5. Cache id_processo list for downstream joins
        df_ids = df.select("id_processo").distinct().cache()
        df_ids.count()

        try:
            # 6. Last relator
            df_hdj        = self.load_historico_distribuicao(df_ids)
            last_relator  = self.calculate_last_relator(df_hdj)
            df = df.join(broadcast(last_relator), on="id_processo", how="left")

            # 7. Acervo flag
            df_acervo = self.load_eproc2g_acervo(df_ids, dt_ref)
            df = df.join(broadcast(df_acervo), on="id_processo", how="left")

            # 8. Last movement (cod_ult_movimento / dth_ult_movimento)
            df_last_mov = self.load_last_movimento_eproc(df_ids)
            df = df.join(broadcast(df_last_mov), on="id_processo", how="left")

            # 9. Build final fact table
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

            self.log_null_counts(df_final, context="eproc final")
            logger.info("Final result: %d rows", df_final.count())

        finally:
            df_ids.unpersist()

        return df_final

    def save(
        self,
        df: DataFrame,
        schema: str = _TARGET_SCHEMA,
        table: str = _TARGET_TABLE,
        ano_mes: str = None,
    ):
        logger.info("Saving ind_sistema=2 | ano_mes=%s → %s.%s", ano_mes, schema, table)
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
    p = argparse.ArgumentParser(description="eProc Repercussão – monthly load to Iceberg (ouro)")
    p.add_argument("--year",  type=int, help="Year  (e.g. 2026)")
    p.add_argument("--month", type=int, help="Month (1-12)")
    p.add_argument("--catalog",        default="glue_catalog")
    p.add_argument("--db-bronze",      default="eproc2g", dest="db_bronze")
    p.add_argument("--db-prata",       default="dw_2inst_prata",        dest="db_prata")
    p.add_argument("--db-ouro",        default="dw_2inst_ouro", dest="db_ouro",
                   help="Database onde vivem tb_ft_acervo_eproc (default: dw_2inst_ouro)")
    p.add_argument("--db-situacao",    default="dw_2inst_ouro",  dest="db_situacao",
                   help="Database da tb_tf_situacao_eproc (onde vive o histórico)")
    p.add_argument("--target-db",      default=_TARGET_SCHEMA, dest="target_db")
    p.add_argument("--target-table",   default=_TARGET_TABLE,  dest="target_table")
    p.add_argument("--s3-json-prefix", default=_S3_JSON_PREFIX, dest="s3_json_prefix",
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
    logger.info("eProc Repercussão | %s → %s | ano_mes=%s", dt_ini, dt_fim, ano_mes)
    logger.info("target: %s.%s", args.target_db, args.target_table)
    logger.info("=" * 70)

    # Load codes from S3 JSON before starting Spark
    s3p = args.s3_json_prefix.rstrip("/")
    codes = _load_json_from_s3(f"{s3p}/repercussao_codes.json")
    logger.info("Loaded repercussao_codes.json: %d suspend, %d unsuspend, %d excluded competencias",
                len(codes["suspend_codes"]), len(codes["unsuspend_codes"]),
                len(codes["excluded_competencias"]))

    spark = _create_spark_session(
        app_name=f"eProc_Repercussao_{ano_mes}",
        executor_memory=args.executor_memory,
        executor_memory_overhead=args.executor_memory_overhead,
        shuffle_partitions=args.shuffle_partitions,
    )
    logger.info("Spark %s ready", spark.version)

    try:
        calc = EprocRepercussaoCalculator(
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
