"""Target table DDL and the DELETE + APPEND write.

Why not overwritePartitions(): ejud m01 writes cod_sistema=1 rows into the same
meta_ano partition. Overwriting would delete them. So eproc2g deletes only its own
(cod_sistema, meta_ano, cod_indicador) rows and appends. ejud m01 must run first.
"""

import logging

from pyspark.sql import DataFrame, SparkSession

from eproc_metacnj.config import Settings

logger = logging.getLogger(__name__)


def ensure_target_table(spark: SparkSession, settings: Settings) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {settings.target_fqn} (
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
        LOCATION '{settings.target_loc}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    logger.info("Ensured target table: %s", settings.target_fqn)


def delete_append(
    spark: SparkSession,
    settings: Settings,
    df: DataFrame,
    cod_sistema: int,
    meta_ano: int,
    cod_indicador: int,
    label: str,
) -> None:
    """Replace this system's rows for one indicator/year, leaving other systems intact."""
    fqn = settings.target_fqn
    logger.info(
        "DELETE %s: meta_ano=%d cod_sistema=%d cod_indicador=%d",
        label, meta_ano, cod_sistema, cod_indicador,
    )
    spark.sql(
        f"DELETE FROM {fqn} "
        f"WHERE cod_sistema = {int(cod_sistema)} "
        f"AND meta_ano = {int(meta_ano)} "
        f"AND cod_indicador = {int(cod_indicador)}"
    )
    logger.info("APPEND %s -> %s", label, fqn)
    df.writeTo(fqn).append()
