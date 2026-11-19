#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_produtividade_emr.py — EJUD Produtividade — 6 indicators → Iceberg (ouro layer)
======================================================================================
Single-script design: no external .py dependencies.
All reads via spark.table() from the Glue catalog (glue_catalog.ejud.*).

Source tables (glue_catalog.ejud.*):
    movimento, movdistribuicao, movdespa, movjulg, movjulgparti
    movimentorelator, movimentofisico, fase
    t_processo, processoassunto, historicomagistrado, tipoprocesso

Target: glue_catalog.dw_2inst_ouro.tb_ft_ejud_produtividade
        Partitioned by (ano_mes)   format YYYY_MM

Indicators written per run (one year-month):
    distribuicao     codfase=[26], movdistribuicao, dedup earliest per coddoc
    redistribuicao   codfase=[36], movdistribuicao, dedup earliest per coddoc
    decisao          codfase=[3,20], movdespa, cod_mag=movdespa.codmag,
                     retired filter, dedup latest per coddoc
    julgado          3 branches (movjulg+movimento+movjulgparti, movdespa codfase=24),
                     union distinct, retired filter, dedup earliest per coddoc
    baixa            all_phases_for_loading, indfaseext='S', LEFT movimentofisico,
                     complex rules, NO deduplication
    emandamento      2-year lookback, latest dist, left_anti finalizing moves,
                     dedup earliest per coddoc

Write strategy (idempotent):
    DELETE FROM target WHERE ano_mes = 'YYYY_MM' AND indicador = '<name>'
    writeTo(...).append()

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_produtividade/ejud/scripts/ejud_produtividade_emr.py \\
      --year-month 2026_03 2>&1 | \\
      grep --line-buffered -E 'ejud_produtividade|ERROR|Exception'

Validate in Athena:
    SELECT indicador, COUNT(*) AS n
    FROM dw_2inst_ouro.tb_ft_ejud_produtividade
    WHERE ano_mes = '2026_03'
    GROUP BY indicador ORDER BY indicador;
"""

import argparse
import json
import logging
import sys
import calendar
from datetime import date, timedelta
from typing import List, Optional, Tuple

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    broadcast, col, coalesce, concat, lit, lpad,
    regexp_replace, row_number, upper, when,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_produtividade_emr")

# ── Catalog / Iceberg constants ───────────────────────────────────────────────
_CATALOG      = "glue_catalog"
_WAREHOUSE    = "s3://sgdai-data-lake"
_DB           = "ejud"
_TARGET_DB    = "dw_2inst_ouro"
_TARGET_TABLE = "tb_ft_ejud_produtividade"
_TARGET_LOC   = f"{_WAREHOUSE}/ouro/{_TARGET_DB}/{_TARGET_TABLE}"
_PRATA_DB     = "dw_2inst_prata"       # judmigracao exclusion table lives here
_CONFIG_S3    = (
    "s3://sgdai-projetos/2instancia/2inst_produtividade/"
    "ejud/json/produtividade_config.json"
)


# ── Utilities ─────────────────────────────────────────────────────────────────
def _norm(c):
    """Strip trailing .0 from numeric-as-string columns (e.g. '12.0' → '12')."""
    return regexp_replace(c.cast("string"), r"\.0$", "")


def _last_day_of_month(y: int, m: int) -> date:
    return date(y, m, calendar.monthrange(y, m)[1])


# ── Config ────────────────────────────────────────────────────────────────────
def _load_config(s3_uri: str) -> dict:
    """Load produtividade_config.json from S3; return 'variaveis' section."""
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    cfg = json.loads(body)
    return cfg.get("variaveis", cfg)


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudProdutividade")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"{_WAREHOUSE}/warehouse")
        .config("spark.sql.defaultCatalog", _CATALOG)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


# ── Iceberg DDL + write ───────────────────────────────────────────────────────
def _ensure_target_table(spark: SparkSession) -> None:
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            cod_documento     STRING,
            cod_cnj           STRING,
            cod_classe        STRING,
            cod_sigilo        STRING,
            cod_assunto_princ INT,
            cod_fase          STRING,
            dth_fase          TIMESTAMP,
            cod_prim_oj       STRING,
            cod_prim_relator  STRING,
            cod_ult_oj        STRING,
            cod_ult_relator   STRING,
            cod_mag           STRING,
            ind_originario    STRING,
            indicador         STRING,
            dth_referencia    TIMESTAMP,
            ano_mes           STRING
        )
        USING iceberg
        PARTITIONED BY (ano_mes)
        LOCATION '{_TARGET_LOC}'
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    logger.info("Ensured target table: %s", fqn)


def _save_indicator(
    spark: SparkSession,
    df: DataFrame,
    indicador: str,
    ano_mes: str,
) -> None:
    fqn = f"{_CATALOG}.{_TARGET_DB}.{_TARGET_TABLE}"
    _ensure_target_table(spark)
    spark.sql(
        f"DELETE FROM {fqn} "
        f"WHERE ano_mes = '{ano_mes}' AND indicador = '{indicador}'"
    )
    logger.info("Writing %s / %s → %s", indicador, ano_mes, fqn)
    n = df.count()
    df.writeTo(fqn).append()
    logger.info("Write complete: %s  rows=%d", indicador, n)


# ── Calculator ────────────────────────────────────────────────────────────────
class EjudProdutividadeCalculator:
    """
    Calculates 6 EJUD productivity indicators for a single year-month.
    All reads via spark.table(glue_catalog.ejud.*).
    """

    def __init__(self, spark: SparkSession, variaveis: dict, db: str = _DB):
        self.spark = spark
        self.db    = db

        self.dist_phases      = variaveis.get("distribuicao",   [26])
        self.redis_phases     = variaveis.get("redistribuicao", [36])
        self.dec_phases       = variaveis.get("decisao",        [3, 20])
        self.julg_phases      = variaveis.get("julgado",        [193, 24])
        em = variaveis.get("emandamento", {})
        self.em_dist_phases   = em.get("distribution_phases", [26, 36])
        self.em_active_phases = em.get("active_phases",       [3, 20, 24, 193])
        self.competencias_exc = variaveis.get("competencias_excluidas",
                                              [2, 9, 10, 11, 12, 14, 15, 19, 20, 21, 22])
        baixa = variaveis.get("baixa", {})
        self.baixa_simple     = baixa.get("simple_phases",         [22, 870, 50011, 50014])
        self.baixa_all        = baixa.get("all_phases_for_loading",
                                          [22, 870, 50011, 50014, 861, 50002, 60, 123, 50123])
        self.baixa_rules      = baixa.get("complex_rules", [])

    # ── Internal table reference ──────────────────────────────────────────────

    def _tbl(self, table: str) -> str:
        return f"{_CATALOG}.{self.db}.{table}"

    # ── Movement loaders ──────────────────────────────────────────────────────

    def _load_mov(
        self,
        table: str,
        dt_from: date,
        dt_to: date,
        phases: Optional[List[int]] = None,
        coddoc_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Load movement table filtered by dthrmov range and optional codfase/coddoc."""
        df = (
            self.spark.table(self._tbl(table))
            .filter(col("dthrmov") >= lit(dt_from))
            .filter(col("dthrmov") < lit(dt_to + timedelta(days=1)))
        )
        if phases:
            df = df.filter(col("codfase").isin(phases))
        if coddoc_df is not None:
            df = df.join(coddoc_df.select("coddoc"), on="coddoc", how="semi")
        return df

    def _load_movrel(
        self,
        dt_from: date,
        dt_to: date,
        coddoc_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Load movimentorelator filtered by dthrmov range, normalised codmagrel/codorgjulg."""
        df = (
            self.spark.table(self._tbl("movimentorelator"))
            .filter(col("dthrmov") >= lit(dt_from))
            .filter(col("dthrmov") < lit(dt_to + timedelta(days=1)))
            .select(
                col("coddoc"),
                col("dthrmov"),
                _norm(col("codmagrel")).alias("codmagrel"),
                _norm(col("codorgjulg")).alias("codorgjulg"),
            )
        )
        if coddoc_df is not None:
            df = df.join(coddoc_df.select("coddoc"), on="coddoc", how="semi")
        return df

    # ── Relator helpers ───────────────────────────────────────────────────────

    def _first_last_relator(
        self, movrel: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """Return (first_df, last_df) — one row per coddoc from movimentorelator."""
        w_asc  = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        w_desc = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        ranked = (
            movrel
            .withColumn("_rn_asc",  row_number().over(w_asc))
            .withColumn("_rn_desc", row_number().over(w_desc))
        )
        first_df = (
            ranked.filter(col("_rn_asc") == 1)
            .select("coddoc",
                    col("codmagrel").alias("cod_prim_relator"),
                    col("codorgjulg").alias("cod_prim_oj"))
        )
        last_df = (
            ranked.filter(col("_rn_desc") == 1)
            .select("coddoc",
                    col("codmagrel").alias("cod_ult_relator"),
                    col("codorgjulg").alias("cod_ult_oj"))
        )
        return first_df, last_df

    def _relator_at_event(
        self,
        events_df: DataFrame,
        movrel_df: DataFrame,
        time_constrained: bool = True,
    ) -> DataFrame:
        """
        Join events with movrel_df to derive cod_mag = latest relator per event.
        Returns events_df with added cod_mag column (drops other movrel cols).
        events_df must have coddoc + dthrmov + codfase.
        """
        if time_constrained:
            joined = events_df.alias("ev").join(
                movrel_df.alias("mr"),
                (col("ev.coddoc") == col("mr.coddoc"))
                & (col("mr.dthrmov") <= col("ev.dthrmov")),
                "left",
            )
        else:
            joined = events_df.alias("ev").join(
                movrel_df.alias("mr"),
                col("ev.coddoc") == col("mr.coddoc"),
                "left",
            )
        w = Window.partitionBy(col("ev.coddoc"), col("ev.dthrmov")).orderBy(
            col("mr.dthrmov").desc()
        )
        return (
            joined.withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select(
                col("ev.coddoc").alias("coddoc"),
                col("ev.dthrmov").alias("dthrmov"),
                col("ev.codfase").alias("codfase"),
                _norm(col("mr.codmagrel")).alias("cod_mag"),
            )
        )

    # ── Retired magistrate filter ─────────────────────────────────────────────

    def _retired_mags(self, dt_ini: date) -> DataFrame:
        """Return distinct codmag_retired of magistrates retired before dt_ini."""
        try:
            return (
                self.spark.table(self._tbl("historicomagistrado"))
                .filter(col("codtipevntmag").cast("int") == 2)
                .filter(col("dtinic").cast("date") < lit(dt_ini))
                .select(_norm(col("codmag")).alias("codmag_retired"))
                .distinct()
            )
        except Exception as exc:
            logger.warning("historicomagistrado unavailable — skipping retired filter: %s", exc)
            return self.spark.createDataFrame([], "codmag_retired STRING")

    # ── Processo enrichment ───────────────────────────────────────────────────

    def _proc_df(self, coddoc_df: DataFrame) -> DataFrame:
        """
        Load t_processo semi-joined to coddoc_df, filter excluded competencias,
        inner join tipoprocesso by codtipproc to get codextcnj as cod_classe
        build cod_cnj / cod_sigilo / ind_originario / cod_classe,
        left-join processoassunto for cod_assunto_princ (numord=1).

        Returns: coddoc, cod_cnj, cod_classe, cod_sigilo, ind_originario, cod_assunto_princ
        """
        coddoc_keys = coddoc_df.select(_norm(col("coddoc")).alias("coddoc")).distinct()
        tp_raw = (
            self.spark.table(self._tbl("t_processo"))
            .withColumn("coddoc", _norm(col("coddoc")))
            .join(coddoc_keys, on="coddoc", how="semi")
            .filter(~_norm(col("codcompt")).cast("int").isin(self.competencias_exc))
        )

        # Filter 1: only active documents (indativo='S' in ejud.documento)
        doc = (
            self.spark.table(self._tbl("documento"))
            .filter(upper(col("indativo")) == "S")
            .select(_norm(col("coddoc")).alias("coddoc"))
            .distinct()
        )
        tp_raw = tp_raw.join(broadcast(doc), on="coddoc", how="inner")

        # Filter 2: exclude migrated coddocs (judmigracao — validated against CNJ datajud)
        try:
            judmig = (
                self.spark.table(f"{_CATALOG}.{_PRATA_DB}.tb_judmigracao")
                .select(_norm(col("coddoc")).alias("coddoc"))
                .distinct()
            )
            tp_raw = tp_raw.join(broadcast(judmig), on="coddoc", how="left_anti")
        except Exception as _exc:
            logger.warning("tb_judmigracao unavailable — skipping judmigracao filter: %s", _exc)

        tp_cols = [c.lower() for c in tp_raw.columns]

        # CNJ number from 6 fields
        num_proc = concat(
            lpad(_norm(col("numsequencial")), 7, "0"),
            lpad(_norm(col("dv")),            2, "0"),
            _norm(col("numano")),
            _norm(col("codjustica")),
            lpad(_norm(col("codregi")),    2, "0"),
            lpad(_norm(col("codlocregi")), 4, "0"),
        )

        # Sigilo: priority 5 > 4 > 1 > 0 (guard missing columns)
        _base = lit("0")
        if "indsegrjust" in tp_cols:
            _base = when(upper(col("indsegrjust")) == "S", lit("1")).otherwise(_base)
        if "indsigil" in tp_cols:
            _base = when(upper(col("indsigil")) == "S", lit("4")).otherwise(_base)
        if "indsupersigil" in tp_cols:
            _base = when(upper(col("indsupersigil")) == "S", lit("5")).otherwise(_base)
        sigilo_expr = _base

        tp_raw = tp_raw.join(self.spark.table(self._tbl("tipoprocesso")).select("codtipproc", "codextcnj"), on="codtipproc", how="left")

        proc = (
            tp_raw
            .withColumn("cod_cnj",        num_proc)
            .withColumn("cod_sigilo",      sigilo_expr)
            .withColumn("ind_originario",
                        when(col("cod_cnj").endswith("0000"), lit("S")).otherwise(lit("N")))
            .withColumn("cod_classe",      _norm(col("codextcnj")))
            .select("coddoc", "cod_cnj", "cod_classe", "cod_sigilo", "ind_originario")
        )

        assunto = (
            self.spark.table(self._tbl("processoassunto"))
            .withColumn("coddoc", _norm(col("coddoc")))
            .join(proc.select("coddoc"), on="coddoc", how="semi")
            .filter(_norm(col("numord")).cast("int") == 1)
            .select(col("coddoc"),
                    _norm(col("codass")).cast("int").alias("cod_assunto_princ"))
            .distinct()
        )

        return (
            proc.join(assunto, on="coddoc", how="left")
            .withColumn("cod_assunto_princ", coalesce(col("cod_assunto_princ"), lit(0)))
        )

    def _enrich(
        self,
        events_df: DataFrame,
        first_df: DataFrame,
        last_df: DataFrame,
    ) -> DataFrame:
        """
        Inner-join events_df with _proc_df (drops excluded competencias),
        left-join first/last relator DataFrames.
        events_df must have coddoc. All coddoc keys normalised to avoid
        float/int/string mismatches.
        """
        events_df = events_df.withColumn("coddoc", _norm(col("coddoc")))
        proc      = self._proc_df(events_df.select("coddoc").distinct())
        proc      = proc.withColumn("coddoc", _norm(col("coddoc")))
        first_df  = first_df.withColumn("coddoc", _norm(col("coddoc")))
        last_df   = last_df.withColumn("coddoc", _norm(col("coddoc")))
        return (
            events_df
            .join(proc,     on="coddoc", how="inner")
            .join(first_df, on="coddoc", how="left")
            .join(last_df,  on="coddoc", how="left")
        )

    # ── Output projection ─────────────────────────────────────────────────────

    def _to_output(
        self,
        df: DataFrame,
        indicador: str,
        dt_ref: date,
        ano_mes: str,
    ) -> DataFrame:
        """
        Project enriched DataFrame to the 16-column target schema.
        Required input columns: coddoc, dthrmov, codfase, cod_mag,
            cod_cnj, cod_classe, cod_sigilo, ind_originario, cod_assunto_princ,
            cod_prim_oj, cod_prim_relator, cod_ult_oj, cod_ult_relator
        """
        cod_fase_expr = (
            when(_norm(col("codfase")) == "24", lit("50193"))
            .when(_norm(col("codfase")) == "20", lit("60003"))
            .otherwise(_norm(col("codfase")))
        )
        return df.select(
            _norm(col("coddoc")).alias("cod_documento"),
            col("cod_cnj"),
            col("cod_classe"),
            col("cod_sigilo"),
            col("cod_assunto_princ").cast("int"),
            cod_fase_expr.alias("cod_fase"),
            col("dthrmov").alias("dth_fase"),
            col("cod_prim_oj"),
            col("cod_prim_relator"),
            col("cod_ult_oj"),
            col("cod_ult_relator"),
            # Fallback: no explicit cod_mag → use most recent relator
            coalesce(col("cod_mag"), col("cod_ult_relator")).alias("cod_mag"),
            col("ind_originario"),
            lit(indicador).alias("indicador"),
            lit(dt_ref).cast("timestamp").alias("dth_referencia"),
            lit(ano_mes).alias("ano_mes"),
        )

    def _dedup(self, df: DataFrame, asc: bool = True) -> DataFrame:
        """Keep one row per coddoc, ordered by dthrmov ASC (asc=True) or DESC."""
        order = col("dthrmov").asc() if asc else col("dthrmov").desc()
        w = Window.partitionBy("coddoc").orderBy(order)
        return (
            df.withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

    # ── Indicators ────────────────────────────────────────────────────────────

    def calc_distribuicao(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_distribuicao  %s → %s", dt_ini, dt_fim)
        year_start = date(dt_ini.year, 1, 1) 
        

        events = self._load_mov("movdistribuicao", dt_ini, dt_fim, self.dist_phases)

        # Exclude distributions cancelled in movcancedistr
        # (match on coddoc + dthrcanceldistr == dthrmov of the distribution event)
        try:
            cancel = (
                self.spark.table(self._tbl("movcancedistr"))
                .select(
                    col("coddoc"),
                    col("dthrcanceldistr").alias("dthrmov"),
                )
                .distinct()
            )
            events = events.join(cancel, on=["coddoc", "dthrmov"], how="left_anti")
        except Exception as _exc:
            logger.warning("movcancedistr unavailable — skipping cancellation filter: %s", _exc)

        coddoc_df  = events.select("coddoc").distinct()
        movrel     = self._load_movrel(year_start, dt_fim, coddoc_df)
        first_df, last_df = self._first_last_relator(movrel)

        # cod_mag = latest relator for the coddoc (no time constraint for distribution)
        enriched = self._relator_at_event(events, movrel, time_constrained=False)
        enriched = self._enrich(enriched, first_df, last_df)
        enriched = self._dedup(enriched, asc=True)
        return self._to_output(enriched, "distribuicao", dt_ref, ano_mes)

    def calc_redistribuicao(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_redistribuicao  %s → %s", dt_ini, dt_fim)
        year_start = date(dt_ini.year, 1, 1)

        events     = self._load_mov("movdistribuicao", dt_ini, dt_fim, self.redis_phases)
        coddoc_df  = events.select("coddoc").distinct()
        movrel     = self._load_movrel(year_start, dt_fim, coddoc_df)
        first_df, last_df = self._first_last_relator(movrel)

        # cod_mag = latest relator <= redistribuicao event time
        enriched = self._relator_at_event(events, movrel, time_constrained=True)
        enriched = self._enrich(enriched, first_df, last_df)
        enriched = self._dedup(enriched, asc=True)
        return self._to_output(enriched, "redistribuicao", dt_ref, ano_mes)

    def calc_decisao(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_decisao  %s → %s", dt_ini, dt_fim)
        year_start = date(dt_ini.year - 1, 1, 1)  # Linux parity: year-1

        # cod_mag comes from movdespa.codmag directly (not from movimentorelator)
        despa = (
            self._load_mov("movdespa", dt_ini, dt_fim, self.dec_phases)
            .select(
                col("coddoc"),
                col("dthrmov"),
                col("codfase"),
                _norm(col("codmag")).alias("cod_mag"),
            )
            .filter(col("cod_mag").isNotNull())
        )

        retired = self._retired_mags(dt_ini)
        despa   = despa.join(retired, despa["cod_mag"] == retired["codmag_retired"], "left_anti")

        coddoc_df = despa.select("coddoc").distinct()
        movrel    = self._load_movrel(year_start, dt_fim, coddoc_df)
        first_df, last_df = self._first_last_relator(movrel)

        enriched = self._enrich(despa, first_df, last_df)
        enriched = self._dedup(enriched, asc=False)  # latest decisao per coddoc
        return self._to_output(enriched, "decisao", dt_ref, ano_mes)

    def calc_julgado(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_julgado  %s → %s", dt_ini, dt_fim)
        year_start = date(dt_ini.year, 1, 1)

        movimentos = self._load_mov("movimento",    dt_ini, dt_fim, self.julg_phases)
        mov_julg   = self._load_mov("movjulg",      dt_ini, dt_fim)
        mov_parti  = self._load_mov("movjulgparti", dt_ini, dt_fim)
        retired    = self._retired_mags(dt_ini)

        # Vista exclusions (IndMagVista = 'S')
        vista_excl = (
            mov_parti.filter(upper(col("indmagvista")) == "S")
            .select(
                _norm(col("coddoc")).alias("v_coddoc"),
                col("dthrmov").alias("v_dthrmov"),
            )
            .distinct()
        )
        julg_no_vista = (
            mov_julg.alias("mj").join(
                vista_excl.alias("v"),
                (col("mj.coddoc") == col("v.v_coddoc"))
                & (col("mj.dthrmov") == col("v.v_dthrmov")),
                "left_anti",
            )
        )

        # IndMagDesig subquery (Branch 1)
        mag_desig = (
            mov_parti.filter(upper(col("indmagdesig")) == "S")
            .select(
                _norm(col("coddoc")).alias("d_coddoc"),
                col("dthrmov").alias("d_dthrmov"),
                _norm(col("codmag")).alias("codmag_desig"),
            )
        )
        # IndMagDesig2 subquery (Branch 2)
        mag_desig2 = (
            mov_parti.filter(upper(col("indmagdesig2")) == "S")
            .select(
                _norm(col("coddoc")).alias("d2_coddoc"),
                col("dthrmov").alias("d2_dthrmov"),
                _norm(col("codmag")).alias("codmag_desig2"),
            )
        )

        # Branch 1 — standard julgamento
        branch1 = (
            julg_no_vista.alias("mj")
            .join(movimentos.alias("m"),
                  (_norm(col("mj.coddoc")) == _norm(col("m.coddoc")))
                  & (col("mj.dthrmov") == col("m.dthrmov")),
                  "inner")
            .join(mag_desig.alias("md"),
                  (_norm(col("mj.coddoc")) == col("md.d_coddoc"))
                  & (col("mj.dthrmov") == col("md.d_dthrmov")),
                  "left")
            .select(
                _norm(col("mj.coddoc")).alias("coddoc"),
                col("mj.dthrmov"),
                col("m.codfase"),
                coalesce(
                    col("md.codmag_desig"),
                    _norm(col("mj.codmagdesig")),
                    _norm(col("mj.codmagrel")),
                ).alias("cod_mag"),
            )
        )

        # Branch 2 — coddec2 IS NOT NULL
        branch2 = (
            julg_no_vista.alias("mj")
            .join(movimentos.alias("m"),
                  (_norm(col("mj.coddoc")) == _norm(col("m.coddoc")))
                  & (col("mj.dthrmov") == col("m.dthrmov")),
                  "inner")
            .join(mag_desig2.alias("md2"),
                  (_norm(col("mj.coddoc")) == col("md2.d2_coddoc"))
                  & (col("mj.dthrmov") == col("md2.d2_dthrmov")),
                  "left")
            .filter(col("mj.coddec2").isNotNull())
            .select(
                _norm(col("mj.coddoc")).alias("coddoc"),
                col("mj.dthrmov"),
                col("m.codfase"),
                coalesce(
                    col("md2.codmag_desig2"),
                    _norm(col("mj.codmagrel")),
                ).alias("cod_mag"),
            )
        )

        # Branch 3 — movdespa codfase=24 (julgamento monocrático via despacho)
        branch3 = (
            self._load_mov("movdespa", dt_ini, dt_fim, [24])
            .select(
                _norm(col("coddoc")).alias("coddoc"),
                col("dthrmov"),
                col("codfase"),
                _norm(col("codmag")).alias("cod_mag"),
            )
        )

        all_events = branch1.union(branch2).union(branch3).distinct()
        all_events = all_events.join(
            retired, all_events["cod_mag"] == retired["codmag_retired"], "left_anti"
        )

        coddoc_df = all_events.select("coddoc").distinct()
        movrel    = self._load_movrel(year_start, dt_fim, coddoc_df)
        first_df, last_df = self._first_last_relator(movrel)

        enriched = self._enrich(all_events, first_df, last_df)
        enriched = self._dedup(enriched, asc=True)
        return self._to_output(enriched, "julgado", dt_ref, ano_mes)

    def calc_baixa(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_baixa  %s → %s", dt_ini, dt_fim)
        year_start = date(dt_ini.year, 1, 1)

        # Load all potential baixa phases, filter by indfaseext='S'
        mov = self._load_mov("movimento", dt_ini, dt_fim, self.baixa_all)
        fase_df = (
            self.spark.table(self._tbl("fase"))
            .filter(upper(col("indfaseext")) == "S")
            .select(_norm(col("codfase")).cast("int").alias("_fase_code"))
            .distinct()
        )
        mov = mov.join(
            broadcast(fase_df),
            col("codfase").cast("int") == col("_fase_code"),
            "inner",
        ).drop("_fase_code")

        # LEFT JOIN movimentofisico for codlocal
        mf = (
            self.spark.table(self._tbl("movimentofisico"))
            .filter(col("dthrmov") >= lit(dt_ini))
            .filter(col("dthrmov") < lit(dt_fim + timedelta(days=1)))
            .select(
                col("coddoc"),
                col("dthrmov"),
                _norm(col("codlocal")).cast("int").alias("codlocal"),
            )
        )
        mov = mov.join(mf, on=["coddoc", "dthrmov"], how="left")

        # Build composite baixa condition from config
        baixa_cond = col("codfase").isin(self.baixa_simple)
        for rule in self.baixa_rules:
            fase_val = rule.get("codfase")
            c1       = rule.get("codcompl1", [])
            c2       = rule.get("codcompl2", [])
            locs     = rule.get("location_codes", [])
            use_loc  = rule.get("requires_location_check", False)

            rc = (
                col("codfase").isin(*fase_val) if isinstance(fase_val, list)
                else (col("codfase") == fase_val)
            )
            if c1:
                rc = rc & col("codcompl1").isin(*c1)
            if c2:
                rc = rc & col("codcompl2").isin(*c2)
            if use_loc and locs:
                rc = rc & col("codlocal").isin(*locs)
            baixa_cond = baixa_cond | rc

        mov_baixa = mov.filter(baixa_cond)

        coddoc_df = mov_baixa.select("coddoc").distinct()
        movrel    = self._load_movrel(year_start, dt_fim, coddoc_df)
        first_df, last_df = self._first_last_relator(movrel)

        # cod_mag = latest relator <= baixa event time
        enriched = self._relator_at_event(mov_baixa, movrel, time_constrained=True)
        enriched = self._enrich(enriched, first_df, last_df)
        # NO deduplication for baixa — multiple baixa records per coddoc are valid
        return self._to_output(enriched, "baixa", dt_ref, ano_mes)

    def calc_emandamento(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_emandamento  %s → %s  (35-year lookback)", dt_ini, dt_fim)
        # 35-year lookback for distributions — oldest observed gap is ~28 years.
        # movrel uses dt_ini.year to match Linux behaviour (line 1254 of ejud_fact_calculator.py).
        lookback_start = date(dt_fim.year - 35, 1, 1)
        year_start     = date(dt_ini.year, 1, 1)

        # Latest distribution per coddoc within the lookback window
        # Also exclude distributions cancelled in movcancedistr
        mov_dist = self._load_mov("movdistribuicao", lookback_start, dt_fim, self.em_dist_phases)
        try:
            cancel = (
                self.spark.table(self._tbl("movcancedistr"))
                .select(col("coddoc"), col("dthrcanceldistr").alias("dthrmov"))
                .distinct()
            )
            mov_dist = mov_dist.join(cancel, on=["coddoc", "dthrmov"], how="left_anti")
        except Exception as _exc:
            logger.warning("movcancedistr unavailable in emandamento: %s", _exc)

        w_latest = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        latest_dist = (
            mov_dist.withColumn("_rn", row_number().over(w_latest))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select("coddoc", "dthrmov", "codfase")
        )

        # Finalizing movements in the same window.
        # CRITICAL: also filter dthrincl <= dt_fim to exclude late-entered records
        # (records with dthrmov inside the month but dthrincl AFTER month-end).
        # Linux reads partitioned parquet by dthrincl date so those records are
        # never loaded during the month run — we must replicate that boundary here.
        fin_mov = (
            self.spark.table(self._tbl("movimento"))
            .filter(col("dthrmov") >= lit(lookback_start))
            .filter(col("dthrmov") < lit(dt_fim + timedelta(days=1)))
            .filter(col("dthrincl") < lit(dt_fim + timedelta(days=1)))  # match Linux partition boundary
            .filter(col("codfase").isin(self.em_active_phases))
            .select(
                col("coddoc").alias("fin_coddoc"),
                col("dthrmov").alias("fin_dthrmov"),
            )
        )

        # Retain only coddocs with NO finalizing move AFTER their latest distribution
        pending = (
            latest_dist.alias("ld").join(
                fin_mov.alias("fm"),
                (col("ld.coddoc") == col("fm.fin_coddoc"))
                & (col("fm.fin_dthrmov") > col("ld.dthrmov")),
                "left_anti",
            )
        )

        coddoc_df = pending.select("coddoc").distinct()
        movrel    = self._load_movrel(year_start, dt_fim, coddoc_df)
        first_df, last_df = self._first_last_relator(movrel)

        enriched = self._relator_at_event(pending, movrel, time_constrained=True)
        enriched = self._enrich(enriched, first_df, last_df)
        enriched = self._dedup(enriched, asc=True)
        return self._to_output(enriched, "emandamento", dt_ref, ano_mes)

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def run(self, year_month: str) -> None:
        """
        Run all 6 indicators for year_month (format YYYY_MM).
        Each indicator deletes its partition slice then appends fresh rows.
        """
        y, m    = int(year_month[:4]), int(year_month[5:])
        dt_ini  = date(y, m, 1)
        dt_fim  = _last_day_of_month(y, m)
        dt_ref  = dt_fim

        logger.info("=" * 70)
        logger.info(
            "ejud_produtividade_emr  year_month=%s  %s → %s",
            year_month, dt_ini, dt_fim,
        )
        logger.info("=" * 70)

        indicators = [
            ("distribuicao",   self.calc_distribuicao),
            ("redistribuicao", self.calc_redistribuicao),
            ("decisao",        self.calc_decisao),
            ("julgado",        self.calc_julgado),
            ("baixa",          self.calc_baixa),
            ("emandamento",    self.calc_emandamento),
        ]

        for name, fn in indicators:
            logger.info("── %s ──", name)
            df = fn(dt_ini, dt_fim, dt_ref, year_month)
            _save_indicator(self.spark, df, name, year_month)

        logger.info("=" * 70)
        logger.info("ejud_produtividade_emr  DONE  year_month=%s", year_month)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    today = date.today()
    default_ym = f"{today.year}_{today.month:02d}"
    p = argparse.ArgumentParser(
        description=(
            "EJUD Produtividade 6 indicators → "
            "Iceberg dw_2inst_ouro.tb_ft_ejud_produtividade"
        )
    )
    p.add_argument(
        "--year-month", default=default_ym, metavar="YYYY_MM",
        help="Target year-month partition (default: current month, e.g. 2026_03)",
    )
    p.add_argument(
        "--config-s3", default=_CONFIG_S3,
        help=f"S3 URI for produtividade_config.json (default: {_CONFIG_S3})",
    )
    p.add_argument(
        "--target-db", default=_TARGET_DB,
        help=f"Glue DB to write into (default: {_TARGET_DB}). "
             "Use 'temp_2inst_ouro' for pre-production testing.",
    )
    p.add_argument(
        "--target-loc", default=_TARGET_LOC,
        help=f"S3 location for target Iceberg table (default: {_TARGET_LOC}).",
    )
    p.add_argument("--executor-memory",          default="12g")
    p.add_argument("--executor-memory-overhead", default="2g")
    p.add_argument("--shuffle-partitions", type=int, default=200)
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    global _TARGET_DB, _TARGET_LOC  # noqa: PLW0603
    _TARGET_DB  = args.target_db
    _TARGET_LOC = args.target_loc

    variaveis = _load_config(args.config_s3)
    spark = _create_spark_session(
        args.executor_memory,
        args.executor_memory_overhead,
        args.shuffle_partitions,
    )
    calc = EjudProdutividadeCalculator(spark=spark, variaveis=variaveis)
    try:
        calc.run(args.year_month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
