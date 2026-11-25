"""Map an indicator DataFrame to the tb_ft_meta_analitico columns.

    id_processo           <- id_processo
    num_processo          <- num_processo
    cod_sistema           <- rules.cod_sistema (2)
    cod_classe_cnj        <- id_classe_judicial (already CNJ code in situacao)
    id_assunto_principal  <- CNJ assunto code (from enrichment)
    cod_competencia       <- processo.cod_competencia (from enrichment)
    dth_evento_indicador  <- event_col (see EVENT_COL in pipeline.py)
    id_sigilo             <- id_sigilo, NULL -> 0
    cod_natureza          <- competencia_judicial (from enrichment)
    cod_ult_oj            <- id_orgao_colegiado
    cod_ult_magistrado    <- NULL (needs historico_distribuicao, not joined)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, lit

ENRICHMENT_COLS = ("cod_competencia", "cod_natureza", "id_assunto_principal")


def to_meta_analitico(
    df: DataFrame,
    cod_sistema: int,
    cod_indicador: int,
    meta_ano: int,
    event_col: str = "dth_inicio",
) -> DataFrame:
    for name in ENRICHMENT_COLS:
        if name not in df.columns:
            df = df.withColumn(name, lit(None).cast("int"))

    return df.select(
        col("id_processo"),
        col("num_processo").cast("string").alias("num_processo"),
        lit(cod_sistema).cast("int").alias("cod_sistema"),
        col("id_classe_judicial").cast("int").alias("cod_classe_cnj"),
        col("id_assunto_principal").cast("int"),
        col("cod_competencia").cast("int"),
        col(event_col).alias("dth_evento_indicador"),
        lit(cod_indicador).cast("int").alias("cod_indicador"),
        coalesce(col("id_sigilo").cast("int"), lit(0)).alias("id_sigilo"),
        col("cod_natureza").cast("int"),
        current_timestamp().alias("dth_atualizacao"),
        col("id_orgao_colegiado").cast("decimal(38,18)").alias("cod_ult_oj"),
        lit(None).cast("decimal(38,18)").alias("cod_ult_magistrado"),
        lit(meta_ano).cast("int").alias("meta_ano"),
    )
