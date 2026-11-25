"""Eligible processes for Meta 01.

A process is eligible when:
    1. Its class is a "conhecimento" class (cod_mni_classe_judicial IN conhecimento_mni_codes,
       via relacao_classe_judicial_mni) and its competencia is not excluded.
    2. It has at least one distribution event (trigger code) up to period_end.

We filter relacao_classe_judicial_mni directly instead of joining mni_classe_judicial
on natureza='Conhecimento': natureza coverage in Iceberg is incomplete and the
eligible count collapses (~400 instead of ~10 000+).
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col

from eproc_metacnj.config import Meta01Rules
from eproc_metacnj.dates import day_end


def conhecimento_processes(
    processo: DataFrame, relacao_classe_mni: DataFrame, rules: Meta01Rules
) -> DataFrame:
    """id_processo (STRING) of conhecimento-class processes with an allowed competencia."""
    classes = (
        relacao_classe_mni
        .filter(col("cod_mni_classe_judicial").isin(rules.conhecimento_mni_codes))
        .select("id_classe_judicial")
        .distinct()
    )
    return (
        processo
        .join(broadcast(classes), on="id_classe_judicial", how="semi")
        .filter(~col("cod_competencia").isin(rules.competencia_exclude))
        .select(col("id_processo").cast("string").alias("id_processo"))
        .distinct()
    )


def distributed_processes(situacao: DataFrame, rules: Meta01Rules, period_end: date) -> DataFrame:
    """id_processo with any distribution event up to the end of period_end."""
    return (
        situacao
        .filter(col("situacao").isin(rules.trigger_codes))
        .filter(col("dth_inicio") <= day_end(period_end))
        .select("id_processo")
        .distinct()
    )


def build_eligible(
    situacao: DataFrame,
    processo: DataFrame,
    relacao_classe_mni: DataFrame,
    rules: Meta01Rules,
    period_end: date,
) -> DataFrame:
    return conhecimento_processes(processo, relacao_classe_mni, rules).join(
        distributed_processes(situacao, rules, period_end), on="id_processo", how="semi"
    )
