"""Extra logging when a step unexpectedly returns zero rows. Never raises."""

import logging
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year

from eproc_metacnj.config import Meta01Rules
from eproc_metacnj.meta01.eligible import conhecimento_processes, distributed_processes

logger = logging.getLogger(__name__)


def diagnose_no_eligible(
    situacao: DataFrame,
    processo: DataFrame,
    relacao_classe_mni: DataFrame,
    rules: Meta01Rules,
    period_end: date,
) -> None:
    logger.warning("DIAGNOSTIC — eligible=0, investigating root cause...")
    try:
        logger.warning("DIAG A — situacao IN trigger_codes counts:")
        (situacao.filter(col("situacao").isin(rules.trigger_codes))
            .groupBy("situacao").count().orderBy("situacao").show(10, truncate=False))

        n_classes = (
            relacao_classe_mni
            .filter(col("cod_mni_classe_judicial").isin(rules.conhecimento_mni_codes))
            .select("id_classe_judicial").distinct().count()
        )
        logger.warning("DIAG B — conhecimento id_classe_judicial in rcjm: %d", n_classes)

        n_proc = conhecimento_processes(processo, relacao_classe_mni, rules).count()
        logger.warning("DIAG C — conhecimento processes (before distribution filter): %d", n_proc)

        logger.warning("DIAG D — cod_competencia distribution in eproc2g.processo:")
        processo.groupBy("cod_competencia").count().orderBy(col("count").desc()).show(20, truncate=False)

        n_dist = distributed_processes(situacao, rules, period_end).count()
        logger.warning("DIAG E — processes with a distribution event: %d", n_dist)
    except Exception as exc:
        logger.warning("DIAGNOSTIC failed: %s", exc)


def diagnose_no_cnm01(situacao: DataFrame, eligible: DataFrame, rules: Meta01Rules) -> None:
    logger.warning(
        "DIAGNOSTIC — cnm01=0 with eligible>0. Check dist_start (%s) vs first sit-88 dates.",
        rules.dist_start,
    )
    try:
        logger.warning("DIAG — sit-88 + trigger events by year for eligible processes:")
        (situacao.join(eligible, on="id_processo", how="semi")
            .filter(col("situacao").isin([rules.PENDENTE] + rules.trigger_codes))
            .groupBy(year(col("dth_inicio")).alias("yr")).count()
            .orderBy("yr").show(20, truncate=False))
    except Exception as exc:
        logger.warning("DIAGNOSTIC failed: %s", exc)
