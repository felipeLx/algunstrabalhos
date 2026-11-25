#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eproc_metacnj_m01_emr.py — EPROC2G Meta 01 -> dw_2inst_ouro.tb_ft_meta_analitico (cod_sistema=2)

Entrypoint only: parse args, build Settings/rules, start Spark, call the pipeline.
Logic lives in the eproc_metacnj package (eproc2g/src/eproc_metacnj).
Business rules: eproc2g/docs/meta01_rules.md.

Orchestration: ejud m01 must run BEFORE this job (see eproc_metacnj/iceberg.py).

Usage (EMR):
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      --py-files s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/scripts/eproc_metacnj.zip \\
      s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/scripts/eproc_metacnj_m01_emr.py \\
      --year 2026 --period-start 2026-01-01 --period-end 2026-12-31

Build the zip:  python eproc2g/build_zip.py
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

try:
    import eproc_metacnj  # noqa: F401  (shipped via --py-files on EMR)
except ImportError:  # local run from the repo
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eproc_metacnj import config
from eproc_metacnj.config import Meta01Rules, load_config, resolve_settings
from eproc_metacnj.meta01 import pipeline
from eproc_metacnj.spark import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("eproc_metacnj_m01_emr")


def parse_args(argv=None) -> argparse.Namespace:
    today = date.today()
    p = argparse.ArgumentParser(
        description="EPROC2G Meta 01 indicators (cod_sistema=2) -> tb_ft_meta_analitico"
    )
    p.add_argument("--year", type=int, default=today.year,
                   help="Reference year, written as meta_ano (default: current year)")
    p.add_argument("--period-start", type=date.fromisoformat, default=date(today.year, 1, 1),
                   help="Period start, ISO date (default: Jan 1 of current year)")
    p.add_argument("--period-end", type=date.fromisoformat, default=date(today.year, 12, 31),
                   help="Period end, ISO date, inclusive (default: Dec 31 of current year)")
    p.add_argument("--config-path", default=config.DEFAULT_CONFIG_PATH,
                   help="meta_config.json (local path or s3://...)")
    p.add_argument("--catalog", default=config.CATALOG)
    p.add_argument("--db-situacao", default=config.DB_SITUACAO,
                   help="Database with tb_tf_situacao_eproc")
    p.add_argument("--db-bronze", default=config.DB_BRONZE,
                   help="eproc2g bronze database used for enrichment")
    p.add_argument("--target-db", default=None,
                   help="Override target database, e.g. temp_2inst_ouro for testing")
    p.add_argument("--target-loc", default=None,
                   help="Override target S3 location (use with --target-db)")
    p.add_argument("--executor-memory", default="12g")
    p.add_argument("--executor-memory-overhead", default="2g")
    p.add_argument("--shuffle-partitions", type=int, default=200)
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config_path)
    settings = resolve_settings(
        cfg,
        catalog=args.catalog,
        db_situacao=args.db_situacao,
        db_bronze=args.db_bronze,
        target_db=args.target_db,
        target_loc=args.target_loc,
    )
    rules = Meta01Rules.from_config(cfg)
    logger.info("Target table: %s at %s", settings.target_fqn, settings.target_loc)

    spark = create_spark_session(
        settings, args.executor_memory, args.executor_memory_overhead, args.shuffle_partitions
    )
    try:
        pipeline.run(spark, settings, rules, args.year, args.period_start, args.period_end)
    except Exception as exc:
        logger.error("Job failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
