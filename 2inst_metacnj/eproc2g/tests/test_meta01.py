from conftest import PERIOD_END, PERIOD_START, ts

from eproc_metacnj.meta01 import indicators
from eproc_metacnj.meta01.eligible import build_eligible
from eproc_metacnj.meta01.schema import to_meta_analitico


def ids(df):
    return sorted(r.id_processo for r in df.select("id_processo").collect())


# ── eligible ──────────────────────────────────────────────────────────────────
def test_eligible_filters_class_competencia_and_distribution(spark, rules, make_situacao):
    processo = spark.createDataFrame(
        [(1, 10, 1), (2, 10, 9), (3, 99, 1), (4, 10, 1), (5, 10, 1)],
        "id_processo INT, id_classe_judicial INT, cod_competencia INT",
    )
    rcjm = spark.createDataFrame([(10, 7), (99, 555)], "id_classe_judicial INT, cod_mni_classe_judicial INT")
    situacao = make_situacao([
        ("1", "2026-03-01 10:00:00", 24),   # ok
        ("2", "2026-03-01 10:00:00", 24),   # excluded competencia
        ("3", "2026-03-01 10:00:00", 24),   # not conhecimento
        ("4", "2027-01-01 00:00:01", 24),   # distributed after period_end
        ("5", "2026-12-31 15:30:00", 24),   # last day, afternoon -> must count
    ])
    eligible = build_eligible(situacao, processo, rcjm, rules, PERIOD_END)
    assert ids(eligible) == ["1", "5"]


# ── cnm01 ─────────────────────────────────────────────────────────────────────
def test_cnm01(rules, make_situacao, make_ids):
    situacao = make_situacao([
        ("A", "2026-01-10 09:00:00", 24), ("A", "2026-01-10 10:00:00", 88),  # new case
        ("B", "2025-05-01 09:00:00", 24), ("B", "2025-05-01 10:00:00", 88),  # first 88 in 2025
        ("B", "2026-02-01 10:00:00", 88),
        ("C", "2026-03-01 10:00:00", 88), ("C", "2026-03-02 10:00:00", 24),  # trigger after 88
        ("D", "2026-04-01 09:00:00", 24), ("D", "2026-04-01 10:00:00", 88),  # not eligible
        ("E", "2026-12-31 14:00:00", 9),  ("E", "2026-12-31 15:00:00", 88),  # last day
    ])
    eligible = make_ids(["A", "B", "C", "E"])
    cnm01 = indicators.build_cnm01(situacao, eligible, rules, PERIOD_START, PERIOD_END)
    assert ids(cnm01) == ["A", "E"]
    row = cnm01.filter("id_processo = 'A'").collect()[0]
    assert row.dth_inicio == ts("2026-01-10 10:00:00")
    assert row.situacao == 88


# ── julgadom01 / PendJulg01TG ─────────────────────────────────────────────────
def test_julgadom01_first_judgment_any_eligible(rules, make_situacao, make_ids):
    situacao = make_situacao([
        ("A", "2026-05-01 10:00:00", 2),
        ("A", "2026-03-01 10:00:00", 27),   # first judgment
        ("B", "2026-06-01 10:00:00", 27),   # backlog case, still counted
        ("C", "2025-06-01 10:00:00", 27),   # outside period
    ])
    julg = indicators.build_julgadom01(situacao, make_ids(["A", "B", "C"]), rules, PERIOD_START, PERIOD_END)
    assert ids(julg) == ["A", "B"]
    assert julg.filter("id_processo = 'A'").collect()[0].dth_inicio == ts("2026-03-01 10:00:00")


def test_pendjulg01(make_ids):
    pend = indicators.build_pendjulg01(make_ids(["A", "E"]), make_ids(["A", "B"]))
    assert ids(pend) == ["E"]


# ── susm01 / desm01 ───────────────────────────────────────────────────────────
def test_suspension(rules, make_situacao):
    cnm01 = make_situacao([
        ("A", "2026-01-10 10:00:00", 88),
        ("E", "2026-02-10 10:00:00", 88),
        ("F", "2026-02-10 10:00:00", 88),
    ])
    situacao = make_situacao([
        ("A", "2026-03-01 10:00:00", 128), ("A", "2026-04-01 10:00:00", 138),  # lifted in period
        ("E", "2026-03-01 10:00:00", 138), ("E", "2026-05-01 10:00:00", 128),  # still suspended
        ("F", "2027-01-05 10:00:00", 128),                                      # after period_end
    ])
    state = indicators.build_suspension_state(situacao, cnm01, rules, PERIOD_END)

    assert ids(indicators.build_susm01(cnm01, state, rules)) == ["E"]

    desm = indicators.build_desm01(cnm01, state, rules, PERIOD_START, PERIOD_END)
    assert ids(desm) == ["A"]
    assert desm.collect()[0].dth_last_susp == ts("2026-04-01 10:00:00")


# ── schema ────────────────────────────────────────────────────────────────────
def test_to_meta_analitico_schema(make_situacao):
    df = make_situacao([("A", "2026-01-10 10:00:00", 88)])
    out = to_meta_analitico(df, cod_sistema=2, cod_indicador=1, meta_ano=2026)
    assert out.columns == [
        "id_processo", "num_processo", "cod_sistema", "cod_classe_cnj", "id_assunto_principal",
        "cod_competencia", "dth_evento_indicador", "cod_indicador", "id_sigilo", "cod_natureza",
        "dth_atualizacao", "cod_ult_oj", "cod_ult_magistrado", "meta_ano",
    ]
    row = out.collect()[0]
    assert (row.cod_sistema, row.cod_indicador, row.meta_ano, row.id_sigilo) == (2, 1, 2026, 0)
    assert row.cod_competencia is None and row.cod_ult_magistrado is None
