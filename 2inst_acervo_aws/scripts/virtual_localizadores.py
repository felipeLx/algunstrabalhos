#!/usr/bin/env python3
"""
Virtual Localizadores — EMR Iceberg writer.

Source:  glue_catalog.ejud.*    (documentosituacaovirtual, localvirtual)
         glue_catalog.eproc2g.* (processo, processo_localizador, localizador_orgao, localizador)
Target:  glue_catalog.dw_2inst_ouro.tb_ft_virtual_localizadores

Usage:
  spark-submit \
    --master yarn --deploy-mode client \
    --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
    s3://sgdai-projetos/2instancia/2inst_acervo_aws/scripts/virtual_localizadores.py \
      --executor-memory 12g --executor-memory-overhead 2g 2>&1 | \
  grep --line-buffered -E 'virtual|ERROR|Exception'
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession

# ── Constants ─────────────────────────────────────────────────────────────────
_CATALOG       = "glue_catalog"
_WAREHOUSE     = "s3://sgdai-data-lake"
_TARGET_SCHEMA = "dw_2inst_ouro"
_TARGET_TABLE  = "tb_ft_virtual_localizadores"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("virtual_localizadores")

# ── SQL ───────────────────────────────────────────────────────────────────────
_SQL_EJUD = """
SELECT
    CAST(m.coddoc AS STRING)              AS cod_documento,
    m.dthrentr                            AS dth_inclusao,
    CAST(m.codlocalvirt AS STRING)        AS id_local_virtual,
    initcap(lower(v.descr))               AS des_local_virtual,
    'ejud'                                AS sistema
FROM ejud.documentosituacaovirtual m
JOIN ejud.localvirtual v
    ON  m.codlocalvirt = v.codlocalvirt
    AND m.codlocal     = v.codlocal
"""

_SQL_EPROC = """
WITH v_localizador_recente AS (
    SELECT
        id_processo,
        dth_inclusao,
        id_localizador_orgao
    FROM eproc2g.processo_localizador
    WHERE dth_inclusao < CAST(current_date AS TIMESTAMP)
      AND dth_desativacao IS NULL
)
SELECT
    p.id_processo                         AS cod_documento,
    pl.dth_inclusao                       AS dth_inclusao,
    CAST(l.id_localizador AS STRING)      AS id_local_virtual,
    initcap(lower(l.des_localizador))     AS des_local_virtual,
    'eproc2g'                             AS sistema
FROM eproc2g.processo p
JOIN v_localizador_recente pl
    ON pl.id_processo = p.id_processo
LEFT JOIN eproc2g.localizador_orgao lo2
    ON pl.id_localizador_orgao = lo2.id_localizador_orgao
LEFT JOIN eproc2g.localizador l
    ON lo2.id_localizador = l.id_localizador
"""


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(app_name: str = "VirtualLocalizadores") -> SparkSession:
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
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


# ── Iceberg write ─────────────────────────────────────────────────────────────
def _save_to_iceberg(df, schema: str, table: str) -> None:
    fqn = f"{_CATALOG}.{schema}.{table}"
    logger.info("Writing to Iceberg: %s", fqn)
    df.writeTo(fqn).using("iceberg").createOrReplace()
    logger.info("Iceberg write complete: %s", fqn)


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Virtual Localizadores — full refresh to Iceberg (dw_2inst_ouro)"
    )
    p.add_argument("--catalog",      default=_CATALOG)
    p.add_argument("--db-ouro",      default=_TARGET_SCHEMA, dest="db_ouro")
    p.add_argument("--target-table", default=_TARGET_TABLE,  dest="target_table")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("=" * 70)
    logger.info("Virtual Localizadores | target: %s.%s.%s",
                args.catalog, args.db_ouro, args.target_table)
    logger.info("=" * 70)

    spark = _create_spark_session()
    logger.info("Spark %s ready", spark.version)

    try:
        logger.info("Processing ejud...")
        df_ejud = spark.sql(_SQL_EJUD)

        logger.info("Processing eproc2g...")
        df_eproc = spark.sql(_SQL_EPROC)

        logger.info("Union both systems...")
        df_final = df_ejud.unionByName(df_eproc)

        _save_to_iceberg(df_final, schema=args.db_ouro, table=args.target_table)

        n = df_final.count()
        logger.info("Done — %d rows written", n)

    except Exception as exc:
        logger.error("Job failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
