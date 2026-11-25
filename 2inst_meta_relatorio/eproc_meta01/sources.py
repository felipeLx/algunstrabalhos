"""All parquet reads. Every other module works on DataFrames only."""

import json
import logging
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from eproc_meta01.constants import PRATA_DIR
from utils.parquet_utils import get_bronze_path, parquet_path

logger = logging.getLogger(__name__)


def variable_files(config_path: str) -> List[str]:
    """Variable parquet names to read, e.g. ["cnm01.parquet", "julgadom01.parquet", ...]."""
    with open(config_path, encoding="utf-8") as f:
        files = json.load(f)["meta_01"]["variables"]["case1"]
    logger.info("Variables config %s -> %s", config_path, files)
    return files


def prata_path(fname: str) -> str:
    return parquet_path(f"{PRATA_DIR}/{fname}")


def read_variables(spark: SparkSession, files: List[str]) -> Dict[str, DataFrame]:
    """{variavel: DataFrame} for each prata file. A file that fails to read is skipped."""
    dfs = {}
    for fname in files:
        path = prata_path(fname)
        logger.info("Reading prata parquet: %s", path)
        try:
            dfs[Path(fname).stem] = spark.read.option("recursiveFileLookup", "true").parquet(path)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", path, exc)
    return dfs


def read_prata_ids(spark: SparkSession, fname: str) -> DataFrame:
    """id_processo column of one prata variable file (used for ind_julgado / ind_baixado)."""
    return spark.read.parquet(prata_path(fname)).select("id_processo")


def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    path = get_bronze_path("eproc2g", table)
    logger.info("Reading eproc2g bronze %s: %s", table, path)
    return spark.read.parquet(path)
