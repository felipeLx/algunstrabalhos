"""Add process attributes from eproc2g bronze tables to the unioned variable rows.

Each add_* function takes the main DataFrame plus the bronze table(s) it needs and
returns the main DataFrame with new columns. No reads happen here (see sources.py).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, row_number, when

# Process attributes taken from bronze processo (any copy in the prata files is replaced).
PROCESSO_COLS = ["cod_competencia", "id_sigilo", "id_assunto_principal", "num_processo", "id_classe_judicial"]


def _first_by(df: DataFrame, order_col: str, descending: bool = False) -> DataFrame:
    """One row per id_processo: earliest (or latest) by order_col."""
    order = col(order_col).desc() if descending else col(order_col).asc()
    w = Window.partitionBy("id_processo").orderBy(order)
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def only_processes_in(table: DataFrame, main: DataFrame) -> DataFrame:
    """Keep only rows of `table` whose id_processo appears in `main` (makes windows cheaper)."""
    return table.join(main.select("id_processo").distinct(), on="id_processo", how="left_semi")


def add_processo(main: DataFrame, processo: DataFrame) -> DataFrame:
    """Replace process attributes with the bronze processo values.

    INNER join: a variable row whose id_processo is missing from processo is dropped.
    """
    main = main.drop(*PROCESSO_COLS, "ind_segredo_justica")
    return main.join(processo.select("id_processo", *PROCESSO_COLS), on="id_processo", how="inner")


def add_natureza(main: DataFrame, competencia: DataFrame) -> DataFrame:
    """cod_natureza from competencia_judicial: sin_civel='S' -> 1, else sin_criminal='S' -> 2, else NULL."""
    comp = competencia.select("cod_competencia", "sin_civel", "sin_criminal")
    return (
        main.join(comp, on="cod_competencia", how="left")
        .withColumn(
            "cod_natureza",
            when(col("sin_civel") == "S", lit(1)).when(col("sin_criminal") == "S", lit(2)),
        )
        .drop("sin_civel", "sin_criminal")
    )


def add_orgao_julgador(main: DataFrame, log_processo: DataFrame) -> DataFrame:
    """cod_prim_oj / cod_ult_oj from log_processo ordered by dth_alteracao.

    cod_ult_oj  = id_orgao_colegiado of the LAST log row.
    cod_prim_oj = id_orgao_colegiado of the FIRST log row, only if that row is not NULL
                  (it is not "first non-null"; see docs, "Known issues").
    """
    logs = only_processes_in(log_processo, main)
    ult = _first_by(logs, "dth_alteracao", descending=True).select(
        "id_processo", col("id_orgao_colegiado").alias("cod_ult_oj")
    )
    prim = (
        _first_by(logs, "dth_alteracao")
        .filter(col("id_orgao_colegiado").isNotNull())
        .select("id_processo", col("id_orgao_colegiado").alias("cod_prim_oj"))
    )
    return main.join(ult, on="id_processo", how="left").join(prim, on="id_processo", how="left")


def add_magistrado(main: DataFrame, historico: DataFrame) -> DataFrame:
    """First / last distribution (magistrado + date) from historico_distribuicao_judicial."""
    hist = only_processes_in(historico.select("id_processo", "cod_magistrado", "dth_distribuicao"), main)
    prim = _first_by(hist, "dth_distribuicao").select(
        "id_processo",
        col("cod_magistrado").alias("cod_prim_magistrado"),
        col("dth_distribuicao").alias("dth_prim_distribuicao"),
    )
    ult = _first_by(hist, "dth_distribuicao", descending=True).select(
        "id_processo",
        col("cod_magistrado").alias("cod_ult_magistrado"),
        col("dth_distribuicao").alias("dth_ult_distribuicao"),
    )
    return main.join(prim, on="id_processo", how="left").join(ult, on="id_processo", how="left")


def add_assunto(main: DataFrame, assunto: DataFrame) -> DataFrame:
    """Translate id_assunto_principal (internal id) -> cod_mni_assunto_judicial (CNJ code).

    INNER join: rows with a NULL or unmapped assunto are dropped.
    """
    a = assunto.select("id_assunto_judicial", "cod_mni_assunto_judicial")
    return (
        main.join(a, main["id_assunto_principal"] == a["id_assunto_judicial"], how="inner")
        .drop("id_assunto_judicial", "id_assunto_principal")
    )


def add_ultimo_evento(main: DataFrame, processo_evento: DataFrame, evento_judicial: DataFrame) -> DataFrame:
    """dth_ult_evento + cod_ult_evento_cnj (CNJ movimento code) of the latest processo_evento row."""
    ev = only_processes_in(processo_evento.select("id_processo", "id_evento_judicial", "dth_evento"), main)
    last = _first_by(ev, "dth_evento", descending=True)
    ej = evento_judicial.select("id_evento_judicial", col("cod_mni_movimento").alias("cod_ult_evento_cnj"))
    last = (
        last.join(ej, on="id_evento_judicial", how="left")
        .select("id_processo", col("dth_evento").alias("dth_ult_evento"), "cod_ult_evento_cnj")
    )
    return main.join(last, on="id_processo", how="left")


def add_flag(main: DataFrame, ids: DataFrame, name: str) -> DataFrame:
    """name = 1 if id_processo is in `ids`, else 0."""
    hit = ids.select("id_processo").distinct().withColumn(name, lit(1))
    return main.join(hit, on="id_processo", how="left").withColumn(
        name, when(col(name) == 1, lit(1)).otherwise(lit(0))
    )
