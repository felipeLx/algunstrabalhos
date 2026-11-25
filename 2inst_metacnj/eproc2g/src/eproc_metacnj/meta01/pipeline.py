"""Meta 01 wiring: read sources, build indicators, write to tb_ft_meta_analitico."""

import logging
from datetime import date
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from eproc_metacnj.config import Meta01Rules, Settings
from eproc_metacnj.iceberg import delete_append, ensure_target_table
from eproc_metacnj.meta01 import diagnostics, indicators
from eproc_metacnj.meta01.eligible import build_eligible
from eproc_metacnj.meta01.schema import to_meta_analitico
from eproc_metacnj.sources import enrich_from_bronze, load_situacao

logger = logging.getLogger(__name__)

# Which column becomes dth_evento_indicador for each variavel.
EVENT_COL = {
    "cnm01":        "dth_inicio",     # first sit-88 (distribution)
    "julgadom01":   "dth_inicio",     # first judgment in period
    "PendJulg01TG": "dth_inicio",     # same as cnm01
    "susm01":       "dth_inicio",     # same as cnm01 (snapshot)
    "desm01":       "dth_last_susp",  # when the suspension was lifted
}


def run(
    spark: SparkSession,
    settings: Settings,
    rules: Meta01Rules,
    year: int,
    period_start: date,
    period_end: date,
) -> Dict[str, int]:
    """Compute and write all Meta 01 indicators. Returns row count per variavel."""
    logger.info("=" * 70)
    logger.info("eproc2g Meta 01  year=%d  period=%s -> %s", year, period_start, period_end)
    logger.info("=" * 70)

    ensure_target_table(spark, settings)

    situacao = load_situacao(spark, settings)
    processo = spark.table(settings.bronze("processo"))
    relacao_classe_mni = spark.table(settings.bronze("relacao_classe_judicial_mni"))

    # 1. Eligible processes (cached: reused by cnm01 and julgadom01)
    eligible = build_eligible(situacao, processo, relacao_classe_mni, rules, period_end).cache()
    n_eligible = eligible.count()
    logger.info("eligible — count=%d", n_eligible)
    if n_eligible == 0:
        diagnostics.diagnose_no_eligible(situacao, processo, relacao_classe_mni, rules, period_end)
        logger.error("No eligible processes found — aborting run.")
        return {}

    # 2. Base indicators, enriched and cached (every other indicator derives from them)
    cnm01 = enrich_from_bronze(
        spark, settings,
        indicators.build_cnm01(situacao, eligible, rules, period_start, period_end),
    ).cache()
    if cnm01.count() == 0:
        diagnostics.diagnose_no_cnm01(situacao, eligible, rules)

    julgadom01 = enrich_from_bronze(
        spark, settings,
        indicators.build_julgadom01(situacao, eligible, rules, period_start, period_end),
    ).cache()
    julgadom01.count()
    eligible.unpersist()  # cnm01 and julgadom01 are materialised

    # 3. Derived indicators
    susp_state = indicators.build_suspension_state(situacao, cnm01, rules, period_end)
    outputs = {
        "cnm01":        cnm01,
        "julgadom01":   julgadom01,
        "PendJulg01TG": indicators.build_pendjulg01(cnm01, julgadom01),
        "susm01":       indicators.build_susm01(cnm01, susp_state, rules),
        "desm01":       indicators.build_desm01(cnm01, susp_state, rules, period_start, period_end),
    }

    # 4. Write each indicator
    counts = {
        variavel: _write_indicator(spark, settings, rules, df, variavel, year)
        for variavel, df in outputs.items()
    }

    cnm01.unpersist()
    julgadom01.unpersist()

    logger.info("Summary — %s", "  ".join(f"{k}={v}" for k, v in counts.items()))
    logger.info("eproc2g Meta 01  DONE  year=%d", year)
    return counts


def _write_indicator(
    spark: SparkSession,
    settings: Settings,
    rules: Meta01Rules,
    df: DataFrame,
    variavel: str,
    year: int,
) -> int:
    cod_indicador = rules.cod_indicador[variavel]
    final = to_meta_analitico(df, rules.cod_sistema, cod_indicador, year, EVENT_COL[variavel])
    n = final.count()
    logger.info("%s — count=%d", variavel, n)
    delete_append(spark, settings, final, rules.cod_sistema, year, cod_indicador, variavel)
    logger.info("%s written", variavel)
    return n
