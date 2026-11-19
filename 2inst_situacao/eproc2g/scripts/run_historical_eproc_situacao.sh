#!/bin/bash
# run_historical_eproc_situacao.sh
#
# Run eproc situacao backfill month-by-month on EMR.
# Requires --deploy-mode client so each spark-submit blocks until done.
#
# Usage:
#   ./run_historical_eproc_situacao.sh [START_YEAR_MONTH] [END_YEAR_MONTH]
#
# Examples:
#   ./run_historical_eproc_situacao.sh 2020-01 2025-12
#   ./run_historical_eproc_situacao.sh 2025-01          # from 2025-01 to current month
#   ./run_historical_eproc_situacao.sh                  # current month only
#
# Environment (override defaults):
#   SPARK_SUBMIT_BIN  spark-submit binary       (default: spark-submit)
#   SPARK_MASTER      Spark master              (default: yarn)
#   DEPLOY_MODE       Spark deploy mode         (default: client)
#   ICEBERG_JAR       Iceberg runtime jar path  (default: EMR standard path)
#   SCRIPT_PATH      S3 path to eproc_situacao_emr.py
#   S3_JSON_PREFIX   S3 prefix where situacoes.json / situacoes_start_end.json live
#   CATALOG          Glue catalog name  (default: glue_catalog)
#   DB_EPROC         Bronze EPROC db    (default: eproc2g)
#   DB_PRATA         Silver/prata db    (default: prata)
#   TARGET_TABLE     Iceberg target     (default: situacao_eproc)

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
CURRENT_YEAR=$(date +%Y)
CURRENT_MONTH=$(date +%-m)

START_ARG="${1:-${CURRENT_YEAR}-$(printf '%02d' ${CURRENT_MONTH})}"
END_ARG="${2:-${CURRENT_YEAR}-$(printf '%02d' ${CURRENT_MONTH})}"

SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
ICEBERG_JAR="${ICEBERG_JAR:-/usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar}"
SCRIPT_PATH="${SCRIPT_PATH:-s3://sgdai-projetos/eproc2g_situacao/eproc_situacao_emr.py}"
S3_JSON_PREFIX="${S3_JSON_PREFIX:-s3://sgdai-projetos/eproc2g_situacao}"
CATALOG="${CATALOG:-glue_catalog}"
DB_EPROC="${DB_EPROC:-eproc2g}"
DB_PRATA="${DB_PRATA:-prata}"
TARGET_TABLE="${TARGET_TABLE:-situacao_eproc}"

# ── Parse YYYY-MM ─────────────────────────────────────────────────────────────
parse_year_month() {
    echo "${1%-*}"   # everything before last '-'
    echo "${1#*-}"   # everything after first '-'
}

START_YEAR=$(echo "$START_ARG" | cut -d'-' -f1)
START_MONTH=$(echo "$START_ARG" | cut -d'-' -f2 | sed 's/^0*//')
END_YEAR=$(echo "$END_ARG" | cut -d'-' -f1)
END_MONTH=$(echo "$END_ARG" | cut -d'-' -f2 | sed 's/^0*//')

echo "=== eproc situacao historical backfill ==="
echo "  Range : ${START_YEAR}-$(printf '%02d' $START_MONTH)  to  ${END_YEAR}-$(printf '%02d' $END_MONTH)"
echo "  Master: ${SPARK_MASTER}"
echo "  Deploy: ${DEPLOY_MODE}"
echo "  JAR   : ${ICEBERG_JAR}"
echo "  Script: ${SCRIPT_PATH}"
echo "=========================================="

if ! command -v "$SPARK_SUBMIT_BIN" >/dev/null 2>&1; then
    echo "ERROR: ${SPARK_SUBMIT_BIN} not found in PATH."
    exit 127
fi

if [ ! -f "$ICEBERG_JAR" ]; then
    echo "ERROR: Iceberg jar not found at ${ICEBERG_JAR}."
    exit 1
fi

# ── Loop ──────────────────────────────────────────────────────────────────────
year=$START_YEAR
month=$START_MONTH

while true; do
    year_month_label="${year}-$(printf '%02d' $month)"
    echo ""
    echo ">>> Processing ${year_month_label}  ($(date '+%Y-%m-%d %H:%M:%S'))"

    "$SPARK_SUBMIT_BIN" \
        --master "$SPARK_MASTER" \
        --deploy-mode "$DEPLOY_MODE" \
        --jars "$ICEBERG_JAR" \
        --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        --conf "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog" \
        --conf "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog" \
        --conf "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
        --conf "spark.sql.catalog.glue_catalog.warehouse=s3://sgdai-data-lake/warehouse" \
        --conf "spark.sql.defaultCatalog=${CATALOG}" \
        --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
        "$SCRIPT_PATH" \
        --year "$year" \
        --month "$month" \
        --catalog "$CATALOG" \
        --db-eproc "$DB_EPROC" \
        --db-prata "$DB_PRATA" \
        --target-table "$TARGET_TABLE" \
        --s3-json-prefix "$S3_JSON_PREFIX"

    rc=$?
    if [ $rc -ne 0 ]; then
        echo "ERROR: spark-submit failed for ${year_month_label} (exit code $rc). Stopping."
        exit $rc
    fi

    echo "<<< Done ${year_month_label}"

    # Advance to next month
    if [ "$year" -eq "$END_YEAR" ] && [ "$month" -eq "$END_MONTH" ]; then
        break
    fi

    month=$((month + 1))
    if [ "$month" -gt 12 ]; then
        month=1
        year=$((year + 1))
    fi
done

echo ""
echo "=== All months completed successfully ==="
