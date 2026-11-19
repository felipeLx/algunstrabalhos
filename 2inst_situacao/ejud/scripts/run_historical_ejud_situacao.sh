#!/bin/bash
# run_historical_ejud_situacao.sh
#
# Run ejud situacao backfill month-by-month on EMR.
# Requires --deploy-mode client so each spark-submit blocks until done.
#
# Requires explicit START and END year-month arguments.
# (aws CLI broken on EMR due to Python conflicts — no Athena auto-discovery)
#
# Quick start on EMR (fetch from S3 and run):
#   hadoop fs -copyToLocal s3://sgdai-projetos/2instancia/2inst_situacao/ejud/scripts/run_historical_ejud_situacao.sh . \
#     && chmod +x run_historical_ejud_situacao.sh \
#     && ./run_historical_ejud_situacao.sh 1988-01 2026-05
#
# Usage:
#   ./run_historical_ejud_situacao.sh [START_YEAR_MONTH] [END_YEAR_MONTH]
#
# Examples:
#   ./run_historical_ejud_situacao.sh 1988-01 2026-05   # full backfill
#   ./run_historical_ejud_situacao.sh 2025-01 2026-05   # partial range
#
# Environment (override defaults):
#   SCRIPT_PATH        S3 path to ejud_situacao_emr.py
#   S3_JSON_PREFIX     S3 prefix where situacoes.json / situacoes_start_end.json live
#   CATALOG            Glue catalog name  (default: glue_catalog)
#   DB_EJUD            Bronze EJUD db     (default: ejud)
#   DB_PRATA           Silver/ouro db     (default: dw_2inst_ouro)
#   TARGET_TABLE       Iceberg target     (default: tb_tf_situacao_ejud)
#   ICEBERG_JAR        Path to iceberg-spark-runtime jar on EMR nodes

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
SCRIPT_PATH="${SCRIPT_PATH:-s3://sgdai-projetos/2instancia/2inst_situacao/ejud/scripts/ejud_situacao_emr.py}"
S3_JSON_PREFIX="${S3_JSON_PREFIX:-s3://sgdai-projetos/2instancia/2inst_situacao/ejud/json}"
CATALOG="${CATALOG:-glue_catalog}"
DB_EJUD="${DB_EJUD:-ejud}"
DB_PRATA="${DB_PRATA:-dw_2inst_ouro}"
TARGET_TABLE="${TARGET_TABLE:-tb_tf_situacao_ejud}"
ICEBERG_JAR="${ICEBERG_JAR:-/usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar}"

# ── Validate args ────────────────────────────────────────────────────────────
if [ $# -lt 2 ]; then
    echo "Usage: $0 START_YEAR_MONTH END_YEAR_MONTH"
    echo "  Ex:  $0 1988-01 2026-05"
    exit 1
fi

START_ARG="$1"
END_ARG="$2"

# ── Parse YYYY-MM ─────────────────────────────────────────────────────────────
START_YEAR=$(echo "$START_ARG" | cut -d'-' -f1)
START_MONTH=$(echo "$START_ARG" | cut -d'-' -f2 | sed 's/^0*//')
END_YEAR=$(echo "$END_ARG" | cut -d'-' -f1)
END_MONTH=$(echo "$END_ARG" | cut -d'-' -f2 | sed 's/^0*//')

echo ""
echo "=== ejud situacao historical backfill ==="
echo "  Range : ${START_YEAR}-$(printf '%02d' $START_MONTH)  to  ${END_YEAR}-$(printf '%02d' $END_MONTH)"
echo "  JAR   : ${ICEBERG_JAR}"
echo "  Script: ${SCRIPT_PATH}"
echo "=========================================="

if ! command -v spark-submit >/dev/null 2>&1; then
    echo "ERROR: spark-submit not found in PATH."
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

    spark-submit \
        --master yarn \
        --deploy-mode client \
        --jars "$ICEBERG_JAR" \
        "$SCRIPT_PATH" \
        --year "$year" \
        --month "$month" \
        --catalog "$CATALOG" \
        --db-ejud "$DB_EJUD" \
        --db-prata "$DB_PRATA" \
        --target-table "$TARGET_TABLE" \
        --s3-json-prefix "$S3_JSON_PREFIX" \
        2>&1 | grep --line-buffered -E 'ejud_situacao_emr|ERROR|Exception' || true

    rc=${PIPESTATUS[0]}
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

echo "=== All months completed successfully ==="
