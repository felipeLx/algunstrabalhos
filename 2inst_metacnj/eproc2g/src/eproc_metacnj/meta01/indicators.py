"""Meta 01 indicators. Each function: DataFrames in -> DataFrame out, no I/O.

Rules are documented in docs/meta01_rules.md. Short version:
    cnm01        first-ever sit-88 in period, preceded by a distribution event
    julgadom01   first judgment (prim_sent or baixa) in period, any eligible process
    PendJulg01TG cnm01 not in julgadom01
    susm01       cnm01 whose last suspension event up to period_end is ADD
    desm01       cnm01 whose last suspension event is REMOVE and falls in period
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number

from eproc_metacnj.config import Meta01Rules
from eproc_metacnj.dates import between_days
from eproc_metacnj.sources import filter_situacao

logger = logging.getLogger(__name__)


def first_per_process(df: DataFrame, order_col: str = "dth_inicio", latest: bool = False) -> DataFrame:
    """Keep one row per id_processo: earliest (default) or latest by order_col."""
    order = col(order_col).desc() if latest else col(order_col).asc()
    w = Window.partitionBy("id_processo").orderBy(order)
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def build_cnm01(
    situacao: DataFrame,
    eligible: DataFrame,
    rules: Meta01Rules,
    period_start: date,
    period_end: date,
) -> DataFrame:
    """Casos novos: first-ever sit-88 falls in the period and a trigger came before it.

    Output keeps the sit-88 row; dth_inicio = date of that first sit-88.
    """
    logger.info("Building cnm01: %s -> %s", period_start, period_end)
    relevant = filter_situacao(
        situacao, [rules.PENDENTE] + rules.trigger_codes,
        rules.dist_start, period_end, only_ids=eligible,
    )

    first_88 = first_per_process(relevant.filter(col("situacao") == rules.PENDENTE))
    triggers = (
        relevant.filter(col("situacao").isin(rules.trigger_codes))
        .select("id_processo", col("dth_inicio").alias("trigger_dt"))
    )
    triggered_88 = (
        first_88
        .join(triggers, on="id_processo", how="inner")
        .filter(col("trigger_dt") <= col("dth_inicio"))
        .dropDuplicates(["id_processo"])
        .drop("trigger_dt")
    )
    return triggered_88.filter(between_days(col("dth_inicio"), period_start, period_end))


def build_julgadom01(
    situacao: DataFrame,
    eligible: DataFrame,
    rules: Meta01Rules,
    period_start: date,
    period_end: date,
) -> DataFrame:
    """First judgment event in the period for every eligible process (backlog included).

    decm (140) and mpum (17, 34) do not occur in tb_tf_situacao_eproc, so they are not used.
    """
    logger.info("Building julgadom01: %s -> %s", period_start, period_end)
    codes = rules.prim_sent_codes + rules.baixa_codes
    return first_per_process(
        filter_situacao(situacao, codes, period_start, period_end, only_ids=eligible)
    )


def build_pendjulg01(cnm01: DataFrame, julgadom01: DataFrame) -> DataFrame:
    """New cases not judged in the period."""
    return cnm01.join(julgadom01.select("id_processo").distinct(), on="id_processo", how="left_anti")


def build_suspension_state(
    situacao: DataFrame, cnm01: DataFrame, rules: Meta01Rules, period_end: date
) -> DataFrame:
    """Last suspension ADD/REMOVE event per cnm01 process up to period_end.

    Columns: id_processo, last_susp_situacao, dth_last_susp.
    """
    codes = rules.suspension_add_codes + rules.suspension_remove_codes
    events = filter_situacao(situacao, codes, rules.dist_start, period_end, only_ids=cnm01)
    return first_per_process(events, latest=True).select(
        "id_processo",
        col("situacao").alias("last_susp_situacao"),
        col("dth_inicio").alias("dth_last_susp"),
    )


def build_susm01(cnm01: DataFrame, susp_state: DataFrame, rules: Meta01Rules) -> DataFrame:
    """cnm01 processes still suspended at period_end (snapshot)."""
    suspended = susp_state.filter(col("last_susp_situacao").isin(rules.suspension_add_codes))
    return cnm01.join(suspended.select("id_processo"), on="id_processo", how="semi")


def build_desm01(
    cnm01: DataFrame,
    susp_state: DataFrame,
    rules: Meta01Rules,
    period_start: date,
    period_end: date,
) -> DataFrame:
    """cnm01 processes whose suspension was lifted during the period (adds dth_last_susp)."""
    lifted = (
        susp_state
        .filter(col("last_susp_situacao").isin(rules.suspension_remove_codes))
        .filter(between_days(col("dth_last_susp"), period_start, period_end))
        .select("id_processo", "dth_last_susp")
    )
    return cnm01.join(lifted, on="id_processo", how="inner")
