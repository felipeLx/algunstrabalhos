"""Shape enriched rows into dw2inst.fato_auxiliar (exactly one row per process)."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from eproc_meta01.constants import META_ANO

COLUMNS = [
    "id_processo", "dth_prim_distribuicao", "dth_ult_distribuicao", "cod_prim_oj", "cod_ult_oj",
    "cod_prim_magistrado", "cod_ult_magistrado", "dth_ult_evento", "cod_ult_evento_cnj",
    "ind_baixado", "ind_julgado", "id_sigilo", "complemento", "meta_ano",
]

INT_COLS = {
    "cod_prim_oj", "cod_ult_oj", "cod_prim_magistrado", "cod_ult_magistrado",
    "cod_ult_evento_cnj", "ind_baixado", "ind_julgado", "id_sigilo",
}


def build(enriched: DataFrame) -> DataFrame:
    """Expects enrichment + add_ultimo_evento + add_flag(ind_baixado / ind_julgado) already applied.

    The input has one row per (process, indicator); all attributes are per-process, so
    dropDuplicates on id_processo keeps one copy.
    """
    df = (
        enriched
        .withColumn("complemento", lit(None).cast("string"))
        .withColumn("meta_ano", lit(META_ANO).cast("int"))
    )
    df = df.select(*[col(c).cast("int").alias(c) if c in INT_COLS else col(c) for c in COLUMNS])
    return df.filter(col("id_processo").isNotNull()).dropDuplicates(["id_processo"])
