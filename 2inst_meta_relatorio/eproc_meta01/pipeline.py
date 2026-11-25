"""Wiring: read prata variables -> enrich from bronze -> build both tables -> write Postgres.

Pure steps live in variables / enrichment / meta_analitico / fato_auxiliar.
This module is the only place that decides order, fallbacks and writes.
"""

import logging
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from eproc_meta01 import enrichment, fato_auxiliar, meta_analitico, sources, variables
from eproc_meta01.constants import DEFAULT_VARIABLES_CONFIG

logger = logging.getLogger(__name__)


def build_enriched(spark: SparkSession, config_path: str) -> DataFrame:
    """Union of all variables (cod_indicador set) + process attributes. One row per (process, indicator)."""
    dfs = sources.read_variables(spark, sources.variable_files(config_path))
    main = variables.union_variables(variables.tag_variables(dfs))

    main = enrichment.add_processo(main, sources.read_bronze(spark, "processo"))
    main = enrichment.add_natureza(main, sources.read_bronze(spark, "competencia_judicial"))
    main = enrichment.add_orgao_julgador(main, sources.read_bronze(spark, "log_processo"))
    main = enrichment.add_magistrado(main, sources.read_bronze(spark, "historico_distribuicao_judicial"))

    try:
        main = enrichment.add_assunto(main, sources.read_bronze(spark, "assunto_judicial"))
    except Exception as exc:
        logger.error("assunto_judicial unavailable, cod_mni_assunto_judicial = NULL: %s", exc)
        main = main.withColumn("cod_mni_assunto_judicial", lit(None).cast("int"))
    return main


def add_fato_extras(spark: SparkSession, main: DataFrame) -> DataFrame:
    """Columns only fato_auxiliar needs. Each one falls back to NULL / 0 if its source is missing."""
    try:
        main = enrichment.add_ultimo_evento(
            main,
            sources.read_bronze(spark, "processo_evento"),
            sources.read_bronze(spark, "evento_judicial"),
        )
    except Exception as exc:
        logger.error("processo_evento / evento_judicial unavailable, last event = NULL: %s", exc)
        main = main.withColumn("dth_ult_evento", lit(None)).withColumn("cod_ult_evento_cnj", lit(None))

    for flag, fname in (("ind_baixado", "baixm01.parquet"), ("ind_julgado", "julgadom01.parquet")):
        try:
            main = enrichment.add_flag(main, sources.read_prata_ids(spark, fname), flag)
        except Exception as exc:
            logger.error("%s unavailable, %s = 0: %s", fname, flag, exc)
            main = main.withColumn(flag, lit(0))
    return main


def build(spark: SparkSession, config_path: str = DEFAULT_VARIABLES_CONFIG) -> Tuple[DataFrame, DataFrame]:
    """(meta_analitico_df, fato_auxiliar_df), nothing written."""
    main = build_enriched(spark, config_path)
    return meta_analitico.build(main), fato_auxiliar.build(add_fato_extras(spark, main))


def run(spark: SparkSession, config_path: Optional[str] = None) -> bool:
    """Build and write both tables. True only if both writes succeed."""
    from eproc_meta01 import writer

    try:
        meta_df, fato_df = build(spark, config_path or DEFAULT_VARIABLES_CONFIG)
        ok_meta = writer.save_meta_analitico(meta_df)
        ok_fato = writer.save_fato_auxiliar(fato_df)
    except Exception:
        logger.exception("eproc2g Meta 01: failed")
        return False

    if ok_meta and ok_fato:
        logger.info("eproc2g Meta 01: done")
    else:
        logger.error("eproc2g Meta 01: write failed (meta_analitico=%s, fato_auxiliar=%s)", ok_meta, ok_fato)
    return ok_meta and ok_fato
