from conftest import ts
from pyspark.sql.functions import lit

from eproc_meta01 import enrichment, fato_auxiliar, meta_analitico, variables


def ids(df):
    return sorted(r.id_processo for r in df.select("id_processo").collect())


def var_df(spark, rows):
    return spark.createDataFrame(
        [(i, ts(d)) for i, d in rows], "id_processo STRING, dth_inicio TIMESTAMP"
    )


# ── variables ────────────────────────────────────────────────────────────────
def test_tag_and_union(spark):
    dfs = {
        "cnm01": var_df(spark, [("A", "2026-01-01 10:00:00"), ("B", "2026-01-02 10:00:00")]),
        "julgadom01": var_df(spark, [("A", "2026-05-01 10:00:00")]),
        "desm01": var_df(spark, [("B", "2026-06-01 10:00:00")]).withColumn("extra", lit(1)),
        "unknown": var_df(spark, [("Z", "2026-06-01 10:00:00")]),
    }
    union = variables.union_variables(variables.tag_variables(dfs))
    got = sorted((r.id_processo, r.cod_indicador) for r in union.collect())
    assert got == [("A", 1), ("A", 2), ("B", 1), ("B", 4), ("B", 5), ("Z", None)]
    pend = union.filter("cod_indicador = 5").collect()[0]
    assert pend.dth_inicio == ts("2026-01-02 10:00:00")   # keeps cnm01 date
    assert "extra" in union.columns                         # missing column -> NULL elsewhere


# ── enrichment ───────────────────────────────────────────────────────────────
def test_add_processo_inner_replaces_attributes(spark):
    main = spark.createDataFrame([("A", "old"), ("X", "old")], "id_processo STRING, num_processo STRING")
    processo = spark.createDataFrame(
        [("A", 10, 0, 7, "123", 99, "ignored")],
        "id_processo STRING, cod_competencia INT, id_sigilo INT, id_assunto_principal INT, "
        "num_processo STRING, id_classe_judicial INT, other STRING",
    )
    out = enrichment.add_processo(main, processo)
    assert ids(out) == ["A"]                  # X dropped (inner)
    row = out.collect()[0]
    assert row.num_processo == "123" and "other" not in out.columns


def test_add_natureza(spark):
    main = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], "id_processo STRING, cod_competencia INT")
    comp = spark.createDataFrame([(1, "S", "N"), (2, "N", "S"), (3, "N", "N")],
                                 "cod_competencia INT, sin_civel STRING, sin_criminal STRING")
    got = {r.id_processo: r.cod_natureza for r in enrichment.add_natureza(main, comp).collect()}
    assert got == {"A": 1, "B": 2, "C": None}


def test_add_orgao_julgador(spark):
    main = spark.createDataFrame([("A",), ("B",)], "id_processo STRING")
    log = spark.createDataFrame(
        [
            ("A", ts("2026-01-01 00:00:00"), 100), ("A", ts("2026-02-01 00:00:00"), 200),
            ("B", ts("2026-01-01 00:00:00"), None), ("B", ts("2026-02-01 00:00:00"), 300),
            ("Q", ts("2026-01-01 00:00:00"), 999),
        ],
        "id_processo STRING, dth_alteracao TIMESTAMP, id_orgao_colegiado INT",
    )
    got = {r.id_processo: (r.cod_prim_oj, r.cod_ult_oj) for r in enrichment.add_orgao_julgador(main, log).collect()}
    assert got == {"A": (100, 200), "B": (None, 300)}   # B: first row NULL -> prim NULL (known issue 3)


def test_add_magistrado(spark):
    main = spark.createDataFrame([("A",), ("A",)], "id_processo STRING")   # two indicators, same process
    hist = spark.createDataFrame(
        [("A", 1, ts("2026-01-01 00:00:00")), ("A", 2, ts("2026-03-01 00:00:00")), ("A", 3, ts("2026-02-01 00:00:00"))],
        "id_processo STRING, cod_magistrado INT, dth_distribuicao TIMESTAMP",
    )
    rows = enrichment.add_magistrado(main, hist).collect()
    assert len(rows) == 2
    assert {(r.cod_prim_magistrado, r.cod_ult_magistrado) for r in rows} == {(1, 2)}
    assert rows[0].dth_ult_distribuicao == ts("2026-03-01 00:00:00")


def test_add_assunto_inner(spark):
    main = spark.createDataFrame([("A", 7), ("B", None), ("C", 8)], "id_processo STRING, id_assunto_principal INT")
    assunto = spark.createDataFrame([(7, 5555)], "id_assunto_judicial INT, cod_mni_assunto_judicial INT")
    out = enrichment.add_assunto(main, assunto)
    assert [(r.id_processo, r.cod_mni_assunto_judicial) for r in out.collect()] == [("A", 5555)]
    assert "id_assunto_principal" not in out.columns


def test_add_ultimo_evento_and_flag(spark):
    main = spark.createDataFrame([("A",), ("B",)], "id_processo STRING")
    pe = spark.createDataFrame(
        [("A", 1, ts("2026-01-01 00:00:00")), ("A", 2, ts("2026-04-01 00:00:00"))],
        "id_processo STRING, id_evento_judicial INT, dth_evento TIMESTAMP",
    )
    ej = spark.createDataFrame([(1, 11), (2, 22)], "id_evento_judicial INT, cod_mni_movimento INT")
    out = enrichment.add_ultimo_evento(main, pe, ej)
    out = enrichment.add_flag(out, spark.createDataFrame([("A",), ("A",)], "id_processo STRING"), "ind_julgado")
    got = {r.id_processo: (r.cod_ult_evento_cnj, r.ind_julgado) for r in out.collect()}
    assert got == {"A": (22, 1), "B": (None, 0)}


# ── final tables ─────────────────────────────────────────────────────────────
def enriched_row(spark):
    return spark.createDataFrame(
        [("A", "123", 10, 5555, 3, ts("2026-01-01 10:00:00"), 1, 0, 1, 200, 2)],
        "id_processo STRING, num_processo STRING, id_classe_judicial INT, cod_mni_assunto_judicial INT, "
        "cod_competencia INT, dth_inicio TIMESTAMP, cod_indicador INT, id_sigilo INT, cod_natureza INT, "
        "cod_ult_oj INT, cod_ult_magistrado INT",
    )


def test_meta_analitico_schema(spark):
    out = meta_analitico.build(enriched_row(spark))
    assert out.columns == meta_analitico.COLUMNS
    r = out.collect()[0]
    assert r.num_processo == "00000000000000000123"
    assert (r.cod_sistema, r.cod_classe_cnj, r.id_assunto_principal, r.meta_ano) == (2, 10, 5555, 2026)
    assert r.dth_evento_indicador == ts("2026-01-01 10:00:00")


def test_fato_auxiliar_one_row_per_process(spark):
    df = spark.createDataFrame(
        [("A", 1), ("A", 2), (None, 1)], "id_processo STRING, cod_indicador INT"
    )
    for c in fato_auxiliar.COLUMNS:
        if c not in df.columns and c not in ("complemento", "meta_ano"):
            df = df.withColumn(c, lit(None))
    out = fato_auxiliar.build(df)
    assert out.columns == fato_auxiliar.COLUMNS
    assert ids(out) == ["A"]
