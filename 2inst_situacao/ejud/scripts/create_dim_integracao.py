#!/usr/bin/env python3
"""
Creates dw_2inst_prata.tb_dm_integracao_ejud_cnj — our own EJUD→CNJ
translation table, replacing the triple-join chain:
    movimento → faseexterna → integracaofasecompl → CNJ code

This table is a FLAT LOOKUP:
    (codfase, codcompl1, codcompl2, codcompl3) → (cod_mni_cnj, ...)

The pipeline (ejud_situacao_emr.py) can join directly on this table
instead of the 3-hop chain through MPS's integracaofasecompl.

WHY WE OWN THIS:
    MPS's integracaofasecompl has gaps (20.8% unresolved), stale rows,
    and missing faseext codes (60005, 60006). We apply:
    - faseexterna overrides (60006→3, 60005→11009)
    - force-to-parent fallback (unresolved complement → parent CNJ node)
    - wildcard filtering (59998/59999 → treated as no-complement)
    - void exclusion (codfase 31, 50010, 50510, 982)

HOW TO UPDATE WHEN CNJ CHANGES:
    1. Download new parametrização CSV from CNJ
    2. Run analyze_gaps.py to find new codes
    3. Update situacoes.json with new mappings
    4. If MPS added new rows to integracaofasecompl, they'll be picked up
       automatically on next run
    5. If new faseexterna codes need overrides, add to FASEEXT_OVERRIDES dict
    6. Re-run this script (daily via Airflow, or manually)

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_situacao/ejud/scripts/create_dim_integracao.py

    # Dry run:
    spark-submit ... create_dim_integracao.py --dry-run
"""

import argparse
import logging
import sys
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, coalesce, col, current_timestamp, lit, when,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("create_dim_integracao")

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION — all hardcoded overrides documented here
# ═══════════════════════════════════════════════════════════════════════════

# Faseexterna overrides: EJUD-only codfaseext → CNJ parent node.
# These faseext codes don't exist in integracaofasecompl, but their
# complements live under the CNJ parent node listed as value.
# Validated via Oracle queries (May 2026) + CNJ movement tree.
FASEEXT_OVERRIDES = {
    60006: 3,      # Decisão em Petição  → Decisão        (6.434 combos)
    60005: 11009,  # Despacho em Petição → Despacho       (52.336 combos)
}

# Void faseext codes: movements that don't map to any CNJ code.
# Excluded from the output table entirely.
VOID_FASEEXT = {
    60007,  # Baixa provisória      (codfase=31, 1.651 combos, not in CNJ)
    50010,  # Baixa diligência      (codfase=50010, 137 combos, EJUD custom, old)
    60011,  # Pedido Desarquivamento(codfase=50510, 54.946, internal 3VP request)
}

# Wildcard complements: placeholders used by EJUD, ignored by MPS.
# Treated as NULL (no complement) during mapping.
WILDCARD_COMPLEMENTS = {59998, 59999}


def create_spark(catalog: str, warehouse: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("CreateDimIntegracaoEjudCnj")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", catalog)
        .getOrCreate()
    )


def tbl(catalog: str, db: str, table: str) -> str:
    return f"{catalog}.{db}.{table}"


def apply_faseext_overrides(df):
    """Remap EJUD-only faseext codes to their CNJ parent nodes."""
    return df.withColumn(
        "codfaseext",
        reduce(
            lambda c, kv: when(c == lit(kv[0]), lit(kv[1])).otherwise(c),
            FASEEXT_OVERRIDES.items(),
            col("codfaseext"),
        ),
    )


def nullify_wildcards(df, *cols):
    """Replace wildcard complement values with NULL."""
    for c in cols:
        for wc in WILDCARD_COMPLEMENTS:
            df = df.withColumn(c, when(col(c) == wc, None).otherwise(col(c)))
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Create tb_dm_integracao_ejud_cnj dimension table"
    )
    parser.add_argument("--catalog", default="glue_catalog")
    parser.add_argument("--db-ejud", default="ejud")
    parser.add_argument("--db-prata", default="dw_2inst_prata")
    parser.add_argument("--target-table", default="tb_dm_integracao_ejud_cnj")
    parser.add_argument("--s3-warehouse", default="s3://sgdai-data-lake/prata")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show schema and sample rows without writing")
    args = parser.parse_args()

    cat = args.catalog
    db = args.db_ejud
    spark = create_spark(cat, args.s3_warehouse)

    try:
        _run(spark, cat, db, args)
    finally:
        spark.stop()


def _run(spark, cat, db, args):
    # ── 1. Read source tables ────────────────────────────────────────────
    logger.info("Reading source tables from %s.%s", cat, db)

    faseexterna_raw = spark.table(tbl(cat, db, "faseexterna"))
    faseexterna = apply_faseext_overrides(faseexterna_raw)

    # Exclude void faseext codes
    void_filter = lit(True)
    for v in VOID_FASEEXT:
        void_filter = void_filter & (col("codfaseext") != v)
    faseexterna = faseexterna.filter(void_filter)

    integ = (
        spark.table(tbl(cat, db, "integracaofasecompl_ejud_cnj"))
        .filter(col("indativo") == "S")
    )

    fase = spark.table(tbl(cat, db, "fase")).select(
        col("codfase").alias("f_codfase"),
        col("descr").alias("descr_fase"),
    )

    complemento = spark.table(tbl(cat, db, "complemento")).select(
        col("codcompl"), col("descr").alias("descr_compl"),
    )

    logger.info("faseexterna: %d rows (after overrides + void filter)",
                faseexterna.count())
    logger.info("integracao (active): %d rows", integ.count())

    # ── 2. Join faseexterna → integracao ─────────────────────────────────
    # This resolves: codfase → codfaseext → integracao rows
    join_cond = col("fe.codfaseext") == col("i.codfasenivel2")

    fe_integ = (
        faseexterna.alias("fe")
        .join(broadcast(integ).alias("i"), join_cond, "inner")
        .select(
            col("fe.codfase"),
            col("fe.codfaseext"),
            # CNJ resolved codes
            col("i.codfasenivel2cnj"),
            col("i.codcompl1nivel3"),
            col("i.codcompl1nivel3cnj"),
            col("i.codcompl2nivel4"),
            col("i.codcompl2nivel4cnj"),
            col("i.codcompl3nivel5"),
            col("i.codcompl3nivel5cnj"),
            col("i.ultimonivelcnj"),
            # Descriptions from integracao
            col("i.descrfasenivel2"),
            col("i.descrcompl1nivel3"),
            col("i.descrcompl2nivel4"),
            col("i.descrcompl3nivel5"),
        )
    )

    # ── 3. Nullify wildcard complements ──────────────────────────────────
    fe_integ = nullify_wildcards(
        fe_integ, "codcompl1nivel3", "codcompl2nivel4", "codcompl3nivel5"
    )

    # ── 4. Compute final CNJ code ────────────────────────────────────────
    # Same logic as ejud_situacao_emr.py: COALESCE(nivel3cnj, nivel2cnj)
    result = fe_integ.select(
        col("codfase"),
        col("codfaseext"),
        # Internal EJUD complement codes (what movimento has)
        col("codcompl1nivel3").alias("codcompl1"),
        col("codcompl2nivel4").alias("codcompl2"),
        col("codcompl3nivel5").alias("codcompl3"),
        # Resolved CNJ movement code
        coalesce(
            col("codcompl1nivel3cnj"), col("codfasenivel2cnj")
        ).alias("cod_mni_cnj"),
        # CNJ complement codes (for situacoes.json matching)
        col("codcompl2nivel4cnj").alias("cnj_comp1"),
        col("codcompl3nivel5cnj").alias("cnj_comp2"),
        # Base CNJ code (nivel-2, for fallback matching)
        col("codfasenivel2cnj").alias("base_cod_mni"),
        col("ultimonivelcnj"),
        # Descriptions for human readability
        col("descrfasenivel2").alias("descr_fase_cnj"),
        col("descrcompl1nivel3").alias("descr_compl1"),
        col("descrcompl2nivel4").alias("descr_compl2"),
        col("descrcompl3nivel5").alias("descr_compl3"),
    )

    # ── 5. Add force-to-parent fallback rows ─────────────────────────────
    # For each codfase that has integracao entries, add a "base" row with
    # NULL complements that maps to the nivel-2 CNJ code. This enables
    # movements with unknown complements to still resolve.
    base_rows = (
        fe_integ
        .select("codfase", "codfaseext", "codfasenivel2cnj", "descrfasenivel2")
        .distinct()
        .select(
            col("codfase"),
            col("codfaseext"),
            lit(None).cast(IntegerType()).alias("codcompl1"),
            lit(None).cast(IntegerType()).alias("codcompl2"),
            lit(None).cast(IntegerType()).alias("codcompl3"),
            col("codfasenivel2cnj").alias("cod_mni_cnj"),
            lit(None).cast(IntegerType()).alias("cnj_comp1"),
            lit(None).cast(IntegerType()).alias("cnj_comp2"),
            col("codfasenivel2cnj").alias("base_cod_mni"),
            lit(2).alias("ultimonivelcnj"),
            col("descrfasenivel2").alias("descr_fase_cnj"),
            lit(None).cast(StringType()).alias("descr_compl1"),
            lit(None).cast(StringType()).alias("descr_compl2"),
            lit(None).cast(StringType()).alias("descr_compl3"),
        )
    )

    # Also add rows for faseexterna codes that have NO integracao match
    # (after overrides). These get a "parent node" row using codfaseext
    # directly as the CNJ code — the force-to-parent strategy.
    fe_no_integ = (
        faseexterna.alias("fe2")
        .join(
            integ.select("codfasenivel2").distinct().alias("i2"),
            col("fe2.codfaseext") == col("i2.codfasenivel2"),
            "left_anti",
        )
        .join(broadcast(fase), col("fe2.codfase") == col("f_codfase"), "left")
        .select(
            col("fe2.codfase"),
            col("fe2.codfaseext"),
            lit(None).cast(IntegerType()).alias("codcompl1"),
            lit(None).cast(IntegerType()).alias("codcompl2"),
            lit(None).cast(IntegerType()).alias("codcompl3"),
            col("fe2.codfaseext").alias("cod_mni_cnj"),
            lit(None).cast(IntegerType()).alias("cnj_comp1"),
            lit(None).cast(IntegerType()).alias("cnj_comp2"),
            col("fe2.codfaseext").alias("base_cod_mni"),
            lit(2).alias("ultimonivelcnj"),
            col("descr_fase").alias("descr_fase_cnj"),
            lit(None).cast(StringType()).alias("descr_compl1"),
            lit(None).cast(StringType()).alias("descr_compl2"),
            lit(None).cast(StringType()).alias("descr_compl3"),
        )
    )

    combined = result.unionByName(base_rows).unionByName(fe_no_integ).distinct()

    # ── 6. Enrich with EJUD fase/complemento descriptions ────────────────
    final = (
        combined
        .join(broadcast(fase), col("codfase") == col("f_codfase"), "left")
        .join(
            broadcast(complemento).alias("ec1"),
            col("codcompl1") == col("ec1.codcompl"), "left",
        )
        .join(
            broadcast(complemento).alias("ec2"),
            col("codcompl2") == col("ec2.codcompl"), "left",
        )
        .join(
            broadcast(complemento).alias("ec3"),
            col("codcompl3") == col("ec3.codcompl"), "left",
        )
        .select(
            # EJUD internal codes (join keys for movimento)
            combined["codfase"],
            combined["codcompl1"],
            combined["codcompl2"],
            combined["codcompl3"],
            # EJUD descriptions
            col("descr_fase").alias("descr_fase_ejud"),
            col("ec1.descr_compl").alias("descr_compl1_ejud"),
            col("ec2.descr_compl").alias("descr_compl2_ejud"),
            col("ec3.descr_compl").alias("descr_compl3_ejud"),
            # Intermediate: faseexterna mapping
            combined["codfaseext"],
            # CNJ resolved codes
            combined["cod_mni_cnj"],
            combined["cnj_comp1"],
            combined["cnj_comp2"],
            combined["base_cod_mni"],
            combined["ultimonivelcnj"],
            # CNJ descriptions (from integracao)
            combined["descr_fase_cnj"],
            combined["descr_compl1"],
            combined["descr_compl2"],
            combined["descr_compl3"],
            # Metadata
            current_timestamp().alias("data_carga"),
        )
        .distinct()
    )

    # ── 7. Write ─────────────────────────────────────────────────────────
    full_target = tbl(cat, args.db_prata, args.target_table)
    row_count = final.count()
    logger.info("Total rows to write: %d", row_count)

    if args.dry_run:
        logger.info("DRY RUN — schema:")
        final.printSchema()
        logger.info("Sample rows:")
        final.orderBy("codfase", "codcompl1").show(50, truncate=False)
        logger.info("Row count: %d", row_count)

        # Stats
        logger.info("--- STATS ---")
        logger.info("Distinct codfase: %d",
                     final.select("codfase").distinct().count())
        logger.info("Distinct cod_mni_cnj: %d",
                     final.select("cod_mni_cnj").distinct().count())
        logger.info("Rows with NULL cod_mni_cnj: %d",
                     final.filter(col("cod_mni_cnj").isNull()).count())
        logger.info("Rows with NULL codcompl1 (base/fallback rows): %d",
                     final.filter(col("codcompl1").isNull()).count())
    else:
        logger.info("Writing to %s", full_target)
        (
            final.writeTo(full_target)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.metadata.compression-codec", "zstd")
            .createOrReplace()
        )
        logger.info("Done. Table created: %s (%d rows)", full_target, row_count)


if __name__ == "__main__":
    main()
