"""Tag each variable with its cod_indicador, derive PendJulg01TG, union everything."""

import logging
from functools import reduce
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from eproc_meta01.constants import COD_INDICADOR

logger = logging.getLogger(__name__)


def with_cod_indicador(df: DataFrame, variavel: str) -> DataFrame:
    """Add cod_indicador for this variable (NULL + warning if the name is unknown)."""
    cod = COD_INDICADOR.get(variavel)
    if cod is None:
        logger.warning("No cod_indicador mapping for %s", variavel)
    return df.withColumn("cod_indicador", lit(cod).cast("int"))


def build_pendjulg01(cnm01: DataFrame, julgadom01: DataFrame) -> DataFrame:
    """PendJulg01TG: new cases (cnm01) not judged in the period (julgadom01)."""
    return cnm01.join(julgadom01.select("id_processo"), on="id_processo", how="left_anti")


def tag_variables(dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """Add cod_indicador to each file and add PendJulg01TG when cnm01 and julgadom01 exist.

    NOTE: rows are NOT de-duplicated by id_processo here. The old script computed a
    deduplicated copy and then never used it; behaviour is kept. See docs, "Known issues".
    """
    tagged = {name: with_cod_indicador(df, name) for name, df in dfs.items()}
    if "cnm01" in dfs and "julgadom01" in dfs:
        tagged["PendJulg01TG"] = with_cod_indicador(
            build_pendjulg01(dfs["cnm01"], dfs["julgadom01"]), "PendJulg01TG"
        )
        logger.info("Created PendJulg01TG (cnm01 minus julgadom01)")
    return tagged


def union_variables(dfs: Dict[str, DataFrame]) -> DataFrame:
    """Stack all variables. Columns missing in one file become NULL."""
    if not dfs:
        raise ValueError("No eproc2g variable data was read")
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs.values())
