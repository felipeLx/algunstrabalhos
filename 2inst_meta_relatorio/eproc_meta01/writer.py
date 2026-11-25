"""Postgres writes. utils.save_to_postgres (psycopg2) is imported lazily so the rest of the
package can be imported and tested without a database driver."""

import logging
import time

from pyspark.sql import DataFrame

from eproc_meta01.constants import COD_SISTEMA, FATO_AUXILIAR_TABLE, META_ANALITICO_TABLE, META_ANO, PG_SCHEMA

logger = logging.getLogger(__name__)


def delete_condition(cod_indicadores) -> str:
    """Rows of this year / system / indicators are deleted before the new ones are inserted."""
    codes = ", ".join(str(c) for c in sorted(cod_indicadores))
    return f"meta_ano = {META_ANO} and cod_sistema = {COD_SISTEMA} and cod_indicador in ({codes})"


def save_meta_analitico(df: DataFrame) -> bool:
    """DELETE existing rows for the indicators present, then INSERT.

    If that fails: dump a diagnostic parquet to /tmp and retry with a plain JDBC append
    (WARNING: the fallback does NOT delete, so old rows stay -> possible duplicates).
    """
    from utils.save_to_postgres import save_df_to_postgres, save_df_to_postgres_no_jar

    codes = [r.cod_indicador for r in df.select("cod_indicador").distinct().collect()]
    cond = delete_condition(codes)
    logger.info("meta_analitico: delete where %s, then insert", cond)
    try:
        if save_df_to_postgres_no_jar(df, schema=PG_SCHEMA, table_name=META_ANALITICO_TABLE,
                                      mode="append", delete_condition=cond):
            return True
    except Exception:
        logger.exception("meta_analitico: delete+insert raised")

    try:
        path = f"/tmp/diagnostic_meta_analitico_{int(time.time())}.parquet"
        df.write.mode("overwrite").parquet(path)
        logger.warning("meta_analitico: diagnostic parquet written to %s", path)
    except Exception as exc:
        logger.warning("meta_analitico: could not write diagnostic parquet: %s", exc)

    try:
        logger.info("meta_analitico: JDBC fallback (append, no delete)")
        save_df_to_postgres(df, schema=PG_SCHEMA, table_name=META_ANALITICO_TABLE, mode_user="append")
        return True
    except Exception:
        logger.exception("meta_analitico: JDBC fallback failed")
        return False


def save_fato_auxiliar(df: DataFrame) -> bool:
    from utils.save_to_postgres import save_parquet_to_postgres

    logger.info("fato_auxiliar: append")
    return bool(save_parquet_to_postgres(df, schema=PG_SCHEMA, table_name=FATO_AUXILIAR_TABLE, mode="append"))
