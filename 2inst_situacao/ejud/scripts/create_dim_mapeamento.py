#!/usr/bin/env python3
"""
Creates dw_2inst_prata.tb_dm_mapeamento_situacao_ejud from situacoes.json
enriched with ejud.fase and ejud.complemento descriptions.

Usage:
    spark-submit --master yarn --deploy-mode client \
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
      s3://sgdai-projetos/2instancia/2inst_situacao/ejud/scripts/create_dim_mapeamento.py
"""

import argparse
import json
import logging
import sys

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, coalesce
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("create_dim_mapeamento")


def load_json_from_s3(s3_uri: str):
    without_prefix = s3_uri.removeprefix("s3://")
    bucket, key = without_prefix.split("/", 1)
    logger.info("Reading %s", s3_uri)
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="glue_catalog")
    parser.add_argument("--db-ejud", default="ejud")
    parser.add_argument("--db-prata", default="dw_2inst_prata")
    parser.add_argument("--target-table", default="tb_dm_mapeamento_situacao_ejud")
    parser.add_argument("--s3-json",
                        default="s3://sgdai-projetos/2instancia/2inst_situacao/ejud/json/situacoes.json")
    parser.add_argument("--s3-warehouse", default="s3://sgdai-data-lake/prata")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cat = args.catalog

    spark = (
        SparkSession.builder
        .appName("CreateDimMapeamentoSituacao")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", args.s3_warehouse)
        .config("spark.sql.defaultCatalog", cat)
        .getOrCreate()
    )

    # ── 1. Load situacoes.json and flatten ────────────────────────────────────
    data = load_json_from_s3(args.s3_json)

    records = []
    for entry in data:
        sit = entry.get("situacao")
        expl = entry.get("explanation", "")
        for mov in entry.get("movimentos", []):
            records.append((
                int(sit) if sit is not None else None,
                expl,
                int(mov["cod_mni_movimento"]),
                int(mov["complemento1"]) if mov.get("complemento1") is not None else None,
                int(mov["complemento2"]) if mov.get("complemento2") is not None else None,
                int(mov["complemento3"]) if mov.get("complemento3") is not None else None,
            ))

    schema = StructType([
        StructField("situacao", IntegerType(), True),
        StructField("explanation", StringType(), True),
        StructField("cod_mni_movimento", IntegerType(), True),
        StructField("complemento1", IntegerType(), True),
        StructField("complemento2", IntegerType(), True),
        StructField("complemento3", IntegerType(), True),
    ])

    mapping_df = spark.createDataFrame(records, schema)
    logger.info("Mapping loaded: %d rules from situacoes.json", len(records))

    # ── 2. Load ejud lookup tables ────────────────────────────────────────────
    integ = (
        spark.table(f"{cat}.{args.db_ejud}.integracaofasecompl_ejud_cnj")
        .filter(col("indativo") == "S")
    )
    fase = spark.table(f"{cat}.{args.db_ejud}.fase")
    complemento = spark.table(f"{cat}.{args.db_ejud}.complemento")

    # ── 3. Join mapping with integration table to get internal codes ──────────
    join_cond = (
        (coalesce(col("i.codcompl1nivel3cnj"), col("i.codfasenivel2cnj")) == col("m.cod_mni_movimento"))
        & (col("m.complemento1").isNull() | (col("i.codcompl2nivel4cnj") == col("m.complemento1")))
        & (col("m.complemento2").isNull() | (col("i.codcompl3nivel5cnj") == col("m.complemento2")))
    )

    enriched = (
        mapping_df.alias("m")
        .join(broadcast(integ).alias("i"), join_cond, "left")
        .join(broadcast(fase).alias("f"), col("i.codfasenivel2") == col("f.codfase"), "left")
        .join(broadcast(complemento).alias("c1"), col("i.codcompl1nivel3") == col("c1.codcompl"), "left")
        .join(broadcast(complemento).alias("c2"), col("i.codcompl2nivel4") == col("c2.codcompl"), "left")
        .join(broadcast(complemento).alias("c3"), col("i.codcompl3nivel5") == col("c3.codcompl"), "left")
        .select(
            col("m.situacao"),
            col("m.explanation"),
            col("m.cod_mni_movimento"),
            col("m.complemento1"),
            col("m.complemento2"),
            col("m.complemento3"),
            col("i.codfasenivel2").alias("codfase"),
            col("f.descr").alias("descr_fase"),
            col("i.codcompl1nivel3").alias("codcompl1"),
            col("c1.descr").alias("descr_compl1"),
            col("i.codcompl2nivel4").alias("codcompl2"),
            col("c2.descr").alias("descr_compl2"),
            col("i.codcompl3nivel5").alias("codcompl3"),
            col("c3.descr").alias("descr_compl3"),
        )
        .distinct()
    )

    # ── 4. Write ──────────────────────────────────────────────────────────────
    full_target = f"{cat}.{args.db_prata}.{args.target_table}"

    if args.dry_run:
        logger.info("DRY RUN — schema:")
        enriched.printSchema()
        enriched.show(30, truncate=False)
        logger.info("Total rows: %d", enriched.count())
    else:
        logger.info("Writing to %s", full_target)
        (
            enriched.writeTo(full_target)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write_compression", "zstd")
            .createOrReplace()
        )
        logger.info("Done. Table created: %s", full_target)

    spark.stop()


if __name__ == "__main__":
    main()
