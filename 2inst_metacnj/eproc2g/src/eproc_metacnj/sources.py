"""Situacao filtering and bronze enrichment.

Pure functions take DataFrames and return DataFrames. Only the `load_*` /
`enrich_from_bronze` functions touch the catalog.
"""

import logging
from datetime import date
from typing import Iterable, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, lit, when

from eproc_metacnj.config import Settings
from eproc_metacnj.dates import between_days

logger = logging.getLogger(__name__)

# Columns of tb_tf_situacao_eproc used downstream.
# id_classe_judicial is already the CNJ MNI code; id_assunto_principal is an
# internal eproc2g id (resolved to CNJ code by enrichment).
SITUACAO_COLS = [
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
]


def load_situacao(spark: SparkSession, settings: Settings) -> DataFrame:
    return spark.table(settings.situacao_fqn)


def filter_situacao(
    situacao: DataFrame,
    codes: Union[int, Iterable[int]],
    date_from: date,
    date_to: date,
    only_ids: Optional[DataFrame] = None,
) -> DataFrame:
    """Situacao rows with situacao IN codes and dth_inicio in [date_from, date_to].

    only_ids: optional DataFrame with id_processo; keeps only those processes.
    """
    codes = [codes] if isinstance(codes, int) else list(codes)
    df = (
        situacao
        .filter(col("situacao").isin(codes))
        .filter(between_days(col("dth_inicio"), date_from, date_to))
        .select(*SITUACAO_COLS)
    )
    if only_ids is not None:
        df = df.join(only_ids.select("id_processo").distinct(), on="id_processo", how="semi")
    return df


# ── Enrichment ────────────────────────────────────────────────────────────────
def resolve_assunto(df: DataFrame, assunto_judicial: DataFrame) -> DataFrame:
    """Replace internal id_assunto_principal with the CNJ code (cod_mni_assunto_judicial)."""
    assunto = (
        assunto_judicial
        .select(
            col("id_assunto_judicial"),
            col("cod_mni_assunto_judicial").cast("int").alias("assunto_cnj"),
        )
        .distinct()
    )
    return (
        df
        .join(broadcast(assunto),
              df["id_assunto_principal"] == assunto["id_assunto_judicial"],
              how="left")
        .drop("id_assunto_judicial", "id_assunto_principal")
        .withColumnRenamed("assunto_cnj", "id_assunto_principal")
    )


def add_competencia_natureza(
    df: DataFrame, processo: DataFrame, competencia_judicial: DataFrame
) -> DataFrame:
    """Add cod_competencia (from processo) and cod_natureza (1=cível, 2=criminal).

    processo.id_processo is INT in bronze; cast to STRING to match situacao.
    Today every eproc2g competencia has sin_civel='S', so cod_natureza is always 1.
    """
    proc = processo.select(
        col("id_processo").cast("string").alias("_proc_id"),
        col("cod_competencia"),
    )
    comp = competencia_judicial.select(
        col("cod_competencia").alias("_comp_id"),
        when(col("sin_civel") == "S", lit(1))
        .when(col("sin_criminal") == "S", lit(2))
        .otherwise(lit(None).cast("int"))
        .alias("cod_natureza"),
    )
    proc_enriched = (
        proc.join(broadcast(comp), proc["cod_competencia"] == comp["_comp_id"], how="left")
        .drop("_comp_id")
    )
    return (
        df.join(broadcast(proc_enriched), df["id_processo"] == proc_enriched["_proc_id"], how="left")
        .drop("_proc_id")
    )


def enrich_from_bronze(spark: SparkSession, settings: Settings, df: DataFrame) -> DataFrame:
    """Apply both enrichments. A missing bronze table degrades to raw/NULL columns."""
    try:
        df = resolve_assunto(df, spark.table(settings.bronze("assunto_judicial")))
        logger.info("assunto_judicial enrichment OK")
    except Exception as exc:
        logger.warning("assunto_judicial enrichment skipped (%s) — keeping raw internal ID", exc)

    try:
        df = add_competencia_natureza(
            df,
            spark.table(settings.bronze("processo")),
            spark.table(settings.bronze("competencia_judicial")),
        )
        logger.info("processo / competencia_judicial enrichment OK")
    except Exception as exc:
        logger.warning(
            "processo/competencia enrichment skipped (%s) — NULL cod_competencia/cod_natureza", exc
        )
        df = (
            df.withColumn("cod_competencia", lit(None).cast("int"))
              .withColumn("cod_natureza", lit(None).cast("int"))
        )
    return df
