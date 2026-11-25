"""Shape enriched rows into dw2inst.meta_analitico (one row per process per indicator)."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, lpad

from eproc_meta01.constants import COD_SISTEMA, META_ANO

COLUMNS = [
    "id_processo", "num_processo", "cod_sistema", "cod_classe_cnj", "id_assunto_principal",
    "cod_competencia", "dth_evento_indicador", "cod_indicador", "id_sigilo", "cod_natureza",
    "dth_atualizacao", "cod_ult_oj", "cod_ult_magistrado", "meta_ano",
]


def build(enriched: DataFrame) -> DataFrame:
    """Rename / cast to the target schema.

    id_assunto_principal in the target holds the CNJ code (cod_mni_assunto_judicial),
    num_processo is left-padded to 20 digits, dth_evento_indicador is the variable's dth_inicio.
    """
    return enriched.select(
        col("id_processo"),
        lpad(col("num_processo"), 20, "0").alias("num_processo"),
        lit(COD_SISTEMA).cast("int").alias("cod_sistema"),
        col("id_classe_judicial").cast("int").alias("cod_classe_cnj"),
        col("cod_mni_assunto_judicial").cast("int").alias("id_assunto_principal"),
        col("cod_competencia").cast("int"),
        col("dth_inicio").alias("dth_evento_indicador"),
        col("cod_indicador").cast("int"),
        col("id_sigilo").cast("int"),
        col("cod_natureza").cast("int"),
        current_timestamp().alias("dth_atualizacao"),
        col("cod_ult_oj").cast("int"),
        col("cod_ult_magistrado").cast("int"),
        lit(META_ANO).alias("meta_ano"),
    )
