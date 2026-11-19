#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eproc2g_produtividade_emr.py — EPROC2G Produtividade — 6 indicators → Iceberg (ouro layer)
============================================================================================
Single-script design: no external .py dependencies.
All reads via spark.table() from the Glue catalog (glue_catalog.eproc2g.*).

EPROC2G started 2024-12. No long lookback needed.

Source tables (glue_catalog.eproc2g.*):
    processo
    processo_evento              (all events; filter: dth_desativacao IS NULL OR id_evento_judicial != 40400400)
    evento_judicial              (MNI code lookup: id_evento_judicial → cod_mni_movimento)
    evento_judicial_mni_complemento_tab  (complemento: id_evento_judicial → cod_mni_complemento_tabelado)
    historico_distribuicao_judicial      (relator history: prim/ult per processo)
    processo_log                 (orgao colegiado history: prim/ult per processo)
    sessao_julgamento_item       (links evento_judicial to session)
    sessao_julgamento_item_magistrado    (magistrate per session, filter cod_status_magistrado='C')
    classe_judicial              (ind_originario via sin_originario)
    relacao_classe_judicial_mni  (cod_classe via cod_mni_classe_judicial)

Target: glue_catalog.dw_2inst_ouro.tb_ft_eproc2g_produtividade
        Partitioned by (ano_mes)  format YYYY_MM

Indicators:
    distribuicao     mni=[26]
    redistribuicao   mni=[36]
    decisao          mni=<decisao list from config>  cod_mag=NULL
    julgado          mni=<julgado list from config>  cod_mag from sessao_julgamento_item_magistrado
    baixa            simple mni + baixa_compl rem rules (complemento_tabelado)
    emandamento      full-history: latest dist per processo, no finalizing event after

Write strategy (idempotent):
    DELETE FROM target WHERE ano_mes = 'YYYY_MM' AND indicador = '<name>'
    writeTo(...).append()

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_produtividade/eproc2g/scripts/eproc2g_produtividade_emr.py \\
      --year-month 2026_03 2>&1 | \\
      grep --line-buffered -E 'eproc2g_produtividade|ERROR|Exception'

Validate in Athena:
    SELECT indicador, COUNT(*) AS n
    FROM dw_2inst_ouro.tb_ft_eproc2g_produtividade
    WHERE ano_mes = '2026_03'
    GROUP BY indicador ORDER BY indicador;
"""

import argparse
import json
import logging
import sys
import calendar
from datetime import date, timedelta
from typing import List, Optional

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    broadcast, col, coalesce, lit, lpad,
    row_number, when,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("eproc2g_produtividade_emr")

# ── Catalog / Iceberg constants ───────────────────────────────────────────────
_CATALOG      = "glue_catalog"
_WAREHOUSE    = "s3://sgdai-data-lake"
_DB           = "eproc2g"
_TARGET_DB    = "dw_2inst_ouro"
_TARGET_TABLE = "tb_ft_eproc2g_produtividade"
_TARGET_LOC   = f"{_WAREHOUSE}/ouro/{_TARGET_DB}/{_TARGET_TABLE}"
_CONFIG_S3    = (
    "s3://sgdai-projetos/2instancia/2inst_produtividade/"
    "eproc2g/json/produtividade_config.json"
)

# EPROC2G system start date — no data before this
_EPROC2G_START = date(2024, 12, 1)

# Event filter: deactivated records (except this sentinel id) are excluded
_EVT_SENTINEL = 40400400


# ── Utilities ─────────────────────────────────────────────────────────────────
def _last_day_of_month(y: int, m: int) -> date:
    return date(y, m, calendar.monthrange(y, m)[1])


# ── Config ────────────────────────────────────────────────────────────────────
def _load_config(s3_uri: str) -> dict:
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
        .appName("Eproc2gProdutividade")
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
class Eproc2gProdutividadeCalculator:
    """
    Calculates 6 EPROC2G productivity indicators for a single year-month.
    All reads via spark.table(glue_catalog.eproc2g.*).
    """

    def __init__(self, spark: SparkSession, variaveis: dict, db: str = _DB):
        self.spark = spark
        self.db    = db

        self.dist_codes       = variaveis.get("distribuicao",   [26])
        self.redis_codes      = variaveis.get("redistribuicao", [36])
        self.dec_codes        = variaveis.get("decisao",        [])
        self.julg_codes       = variaveis.get("julgado",        [])
        self.baixa_codes      = variaveis.get("baixa",          [246, 22, 488, 12186, 11384, 11385])
        self.baixa_compl      = variaveis.get("baixa_compl",    {})
        self.competencias_exc = variaveis.get("competencias_excluidas", [2, 9, 2026])

        # All MNI codes that "finalize" a process for emandamento purposes
        # (julgado + baixa + decisao codes + baixa_compl rem movimento codes)
        baixa_rem_codes = [
            r.get("cod_mni_movimento")
            for r in self.baixa_compl.get("rem", [])
            if r.get("cod_mni_movimento")
        ]
        self._exclusao_codes = list(
            set(self.julg_codes) | set(self.baixa_codes) | set(self.dec_codes)
            | set(baixa_rem_codes)
        )

    # ── Internal table reference ──────────────────────────────────────────────

    def _tbl(self, table: str) -> str:
        return f"{_CATALOG}.{self.db}.{table}"

    # ── Core loaders ─────────────────────────────────────────────────────────

    def _load_eventos(
        self,
        dt_from: date,
        dt_to: date,
        mni_codes: Optional[List[int]] = None,
    ) -> DataFrame:
        """
        Load processo_evento rows in [dt_from, dt_to], apply dth_desativacao filter,
        join evento_judicial for cod_mni_movimento, left-join mni_complemento_tab
        for cod_mni_complemento_tabelado.

        Returns: id_processo, seq_processo, id_evento_judicial, dth_evento,
                 cod_mni_movimento, cod_mni_complemento_tabelado
        """
        pe = (
            self.spark.table(self._tbl("processo_evento"))
            .filter(
                col("dth_desativacao").isNull()
                | (col("id_evento_judicial") != _EVT_SENTINEL)
            )
            .filter(col("dth_evento") >= lit(dt_from))
            .filter(col("dth_evento") < lit(dt_to + timedelta(days=1)))
            .select("id_processo", "seq_processo", "id_evento_judicial", "dth_evento")
        )

        ej = (
            self.spark.table(self._tbl("evento_judicial"))
            .select("id_evento_judicial", "cod_mni_movimento")
        )

        compl = (
            self.spark.table(self._tbl("evento_judicial_mni_complemento_tab"))
            .select("id_evento_judicial", "cod_mni_complemento_tabelado")
        )

        df = (
            pe.join(ej, on="id_evento_judicial", how="inner")
            .join(compl, on="id_evento_judicial", how="left")
        )

        if mni_codes:
            df = df.filter(col("cod_mni_movimento").isin(mni_codes))

        return df

    def _load_processo(self) -> DataFrame:
        """
        Load processo filtered by excluded competencias.
        Joins relacao_classe_judicial_mni and classe_judicial.
        Returns: id_processo, seq_processo, cod_cnj, cod_classe, cod_sigilo,
                 cod_assunto_princ, ind_originario, id_orgao_colegiado
        """
        proc = (
            self.spark.table(self._tbl("processo"))
            .select(
                "id_processo", "seq_processo", "num_processo",
                "id_orgao_colegiado", "id_assunto_principal",
                "id_classe_judicial", "cod_competencia", "id_sigilo",
            )
            .filter(~col("cod_competencia").isin(self.competencias_exc))
        )

        rcjm = (
            self.spark.table(self._tbl("relacao_classe_judicial_mni"))
            .select("id_classe_judicial", "cod_mni_classe_judicial")
        )

        cj = (
            self.spark.table(self._tbl("classe_judicial"))
            .select("id_classe_judicial", "sin_originario")
        )

        proc = (
            proc
            .join(rcjm, on="id_classe_judicial", how="left")
            .join(cj,   on="id_classe_judicial", how="left")
            .withColumn(
                "ind_originario",
                when(col("sin_originario") == "S", lit("S")).otherwise(lit("N")),
            )
            .drop("sin_originario")
            .withColumn("cod_cnj", lpad(col("num_processo").cast("string"), 20, "0"))
        )

        return proc.select(
            col("id_processo").cast("string"),
            col("seq_processo"),
            col("cod_cnj"),
            col("cod_mni_classe_judicial").cast("string").alias("cod_classe"),
            col("id_sigilo").cast("string").alias("cod_sigilo"),
            col("id_assunto_principal").cast("int").alias("cod_assunto_princ"),
            col("ind_originario"),
            col("id_orgao_colegiado"),
        )

    def _load_hdj(self) -> DataFrame:
        """
        historico_distribuicao_judicial → first/last relator per (id_processo, seq_processo).
        Returns: id_processo (string), seq_processo, cod_prim_relator, cod_ult_relator
        """
        hdj = (
            self.spark.table(self._tbl("historico_distribuicao_judicial"))
            .select("id_processo", "seq_processo", "dth_distribuicao", "cod_magistrado")
        )

        w_asc  = Window.partitionBy("id_processo", "seq_processo").orderBy(col("dth_distribuicao").asc())
        w_desc = Window.partitionBy("id_processo", "seq_processo").orderBy(col("dth_distribuicao").desc())

        prim = (
            hdj.withColumn("rn", row_number().over(w_asc)).filter(col("rn") == 1)
            .select(
                col("id_processo").cast("string"),
                col("seq_processo"),
                col("cod_magistrado").cast("string").alias("cod_prim_relator"),
            )
        )
        ult = (
            hdj.withColumn("rn", row_number().over(w_desc)).filter(col("rn") == 1)
            .select(
                col("id_processo").cast("string"),
                col("seq_processo"),
                col("cod_magistrado").cast("string").alias("cod_ult_relator"),
            )
        )

        return prim.join(ult, on=["id_processo", "seq_processo"], how="outer")

    def _load_log_oj(self) -> DataFrame:
        """
        processo_log → first/last orgao colegiado per id_processo.
        Returns: id_processo (string), cod_prim_oj, cod_ult_oj
        """
        log = (
            self.spark.table(self._tbl("processo_log"))
            .select("id_processo", "id_orgao_colegiado", "dth_alteracao")
        )

        w_asc  = Window.partitionBy("id_processo").orderBy(col("dth_alteracao").asc())
        w_desc = Window.partitionBy("id_processo").orderBy(col("dth_alteracao").desc())

        prim = (
            log.withColumn("rn", row_number().over(w_asc)).filter(col("rn") == 1)
            .select(
                col("id_processo").cast("string"),
                col("id_orgao_colegiado").cast("string").alias("cod_prim_oj"),
            )
        )
        ult = (
            log.withColumn("rn", row_number().over(w_desc)).filter(col("rn") == 1)
            .select(
                col("id_processo").cast("string"),
                col("id_orgao_colegiado").cast("string").alias("cod_ult_oj"),
            )
        )

        return prim.join(ult, on="id_processo", how="outer")

    def _load_julgado_mag(self, events_df: DataFrame) -> DataFrame:
        """
        Join events_df (must have id_processo, seq_processo, id_evento_judicial)
        with sessao_julgamento_item + sessao_julgamento_item_magistrado
        to derive cod_mag for julgado events.

        id_processo_evento_resultado in sessao_julgamento_item maps to
        id_evento_judicial in processo_evento (via evento_judicial).

        Returns events_df + cod_mag column (NULL when no session magistrate found).
        """
        sji = (
            self.spark.table(self._tbl("sessao_julgamento_item"))
            .select(
                col("id_processo_evento_resultado").cast("string").alias("iev_key"),
                col("id_processo").cast("string").alias("sji_id_processo"),
                col("seq_processo").cast("string").alias("sji_seq_processo"),
                "id_sessao_julgamento_item",
            )
        )

        sjm = (
            self.spark.table(self._tbl("sessao_julgamento_item_magistrado"))
            # Exclude cancelled magistrate records
            .filter(col("dth_desativacao").isNull())
            # cod_status='C' = relator correspondente;
            # sin_voto_divergente='S' = magistrate with divergent vote (also counts as cod_mag)
            .filter(
                (col("cod_status_magistrado") == "C")
                | (col("sin_voto_divergente") == "S")
            )
            .select(
                "id_sessao_julgamento_item",
                col("id_orgao_juizo_magistrado").cast("string").alias("cod_mag"),
            )
        )

        mag = (
            sji.join(sjm, on="id_sessao_julgamento_item", how="inner")
            .select("iev_key", "sji_id_processo", "sji_seq_processo", "cod_mag")
        )

        return (
            events_df.alias("ev")
            .join(
                mag.alias("mg"),
                (col("ev.id_evento_judicial").cast("string") == col("mg.iev_key"))
                & (col("ev.id_processo").cast("string")  == col("mg.sji_id_processo"))
                & (col("ev.seq_processo").cast("string")  == col("mg.sji_seq_processo")),
                "left",
            )
            .select(col("ev.*"), col("mg.cod_mag"))
        )

    def _enrich(self, events_df: DataFrame) -> DataFrame:
        """
        Inner-join with processo (applies competencia filter),
        left-join hdj relator and log oj.
        events_df must have id_processo (string), seq_processo.
        Falls back cod_prim_oj / cod_ult_oj to processo.id_orgao_colegiado.
        """
        proc   = self._load_processo()
        hdj    = self._load_hdj()
        log_oj = self._load_log_oj()

        # Normalise key types before join
        ev = events_df.withColumn("id_processo", col("id_processo").cast("string"))

        df = (
            ev.join(proc,   on=["id_processo", "seq_processo"], how="inner")
            .join(hdj,    on=["id_processo", "seq_processo"], how="left")
            .join(log_oj, on="id_processo",                  how="left")
        )

        # Fallback: use processo.id_orgao_colegiado when log is absent
        df = (
            df
            .withColumn("cod_prim_oj",
                        coalesce(col("cod_prim_oj"), col("id_orgao_colegiado").cast("string")))
            .withColumn("cod_ult_oj",
                        coalesce(col("cod_ult_oj"),  col("id_orgao_colegiado").cast("string")))
        )
        return df

    def _to_output(
        self,
        df: DataFrame,
        indicador: str,
        dt_ref: date,
        ano_mes: str,
    ) -> DataFrame:
        """Project enriched DataFrame to the 16-column target schema.
        cod_mag falls back to cod_ult_relator when no session magistrate was found.
        """
        return df.select(
            col("id_processo").cast("string").alias("cod_documento"),
            col("cod_cnj"),
            col("cod_classe"),
            col("cod_sigilo"),
            col("cod_assunto_princ").cast("int"),
            col("cod_mni_movimento").cast("string").alias("cod_fase"),
            col("dth_evento").alias("dth_fase"),
            col("cod_prim_oj"),
            col("cod_prim_relator"),
            col("cod_ult_oj"),
            col("cod_ult_relator"),
            # Fallback: no session magistrate → use most recent relator
            coalesce(col("cod_mag"), col("cod_ult_relator")).alias("cod_mag"),
            col("ind_originario"),
            lit(indicador).alias("indicador"),
            lit(dt_ref).cast("timestamp").alias("dth_referencia"),
            lit(ano_mes).alias("ano_mes"),
        )

    # ── Indicators ────────────────────────────────────────────────────────────

    def calc_distribuicao(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_distribuicao  %s → %s", dt_ini, dt_fim)
        events = self._load_eventos(dt_ini, dt_fim, self.dist_codes)
        events = events.withColumn("cod_mag", lit(None).cast("string"))
        enriched = self._enrich(events)
        return self._to_output(enriched, "distribuicao", dt_ref, ano_mes).dropDuplicates()

    def calc_redistribuicao(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_redistribuicao  %s → %s", dt_ini, dt_fim)
        events = self._load_eventos(dt_ini, dt_fim, self.redis_codes)
        events = events.withColumn("cod_mag", lit(None).cast("string"))
        enriched = self._enrich(events)
        return self._to_output(enriched, "redistribuicao", dt_ref, ano_mes).dropDuplicates()

    def calc_decisao(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_decisao  %s → %s", dt_ini, dt_fim)
        events = self._load_eventos(dt_ini, dt_fim, self.dec_codes)
        # Decisao: cod_mag not derived from sessao magistrate
        events = events.withColumn("cod_mag", lit(None).cast("string"))
        enriched = self._enrich(events)
        return self._to_output(enriched, "decisao", dt_ref, ano_mes).dropDuplicates()

    def calc_julgado(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_julgado  %s → %s", dt_ini, dt_fim)
        events = self._load_eventos(dt_ini, dt_fim, self.julg_codes)
        # Julgado: cod_mag from sessao_julgamento_item_magistrado (cod_status='C')
        events = self._load_julgado_mag(events)
        enriched = self._enrich(events)
        return self._to_output(enriched, "julgado", dt_ref, ano_mes).dropDuplicates()

    def calc_baixa(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_baixa  %s → %s", dt_ini, dt_fim)

        # All potential baixa MNI codes (simple + baixa_compl rem movimento codes)
        rem_codes = [
            r.get("cod_mni_movimento")
            for r in self.baixa_compl.get("rem", [])
            if r.get("cod_mni_movimento")
        ]
        all_baixa_codes = list(set(self.baixa_codes) | set(rem_codes))

        events = self._load_eventos(dt_ini, dt_fim, all_baixa_codes)

        # Build composite baixa condition
        cond = col("cod_mni_movimento").isin(self.baixa_codes)
        for rule in self.baixa_compl.get("rem", []):
            mni      = rule.get("cod_mni_movimento")
            tab_vals = rule.get("cod_mni_complemento_tabelado", [])
            if mni and tab_vals:
                rc = (col("cod_mni_movimento") == mni) & col("cod_mni_complemento_tabelado").isin(tab_vals)
                cond = cond | rc

        events = events.filter(cond)
        events = events.withColumn("cod_mag", lit(None).cast("string"))
        enriched = self._enrich(events)
        # NO dedup for baixa — multiple baixa records per processo are valid
        return self._to_output(enriched, "baixa", dt_ref, ano_mes)

    def calc_emandamento(
        self, dt_ini: date, dt_fim: date, dt_ref: date, ano_mes: str
    ) -> DataFrame:
        logger.info("calc_emandamento  %s → %s  (full eproc2g history)", dt_ini, dt_fim)

        # Load all distributions from eproc2g start up to end of target month
        all_dist = self._load_eventos(_EPROC2G_START, dt_fim, self.dist_codes + self.redis_codes)

        # Latest distribution per id_processo
        w_latest = Window.partitionBy("id_processo").orderBy(col("dth_evento").desc())
        latest_dist = (
            all_dist.withColumn("_rn", row_number().over(w_latest))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # Finalizing events (julgado + baixa + decisao + baixa_compl rem)
        # Also apply end-of-month boundary to exclude late-entered events
        fin_events = (
            self.spark.table(self._tbl("processo_evento"))
            .filter(
                col("dth_desativacao").isNull()
                | (col("id_evento_judicial") != _EVT_SENTINEL)
            )
            .filter(col("dth_evento") >= lit(_EPROC2G_START))
            .filter(col("dth_evento") < lit(dt_fim + timedelta(days=1)))
            .select("id_processo", "id_evento_judicial", "dth_evento")
        )
        ej = self.spark.table(self._tbl("evento_judicial")).select("id_evento_judicial", "cod_mni_movimento")
        compl = self.spark.table(self._tbl("evento_judicial_mni_complemento_tab")).select(
            "id_evento_judicial", "cod_mni_complemento_tabelado"
        )
        fin_events = (
            fin_events
            .join(ej,    on="id_evento_judicial", how="inner")
            .join(compl, on="id_evento_judicial", how="left")
        )

        # Build finalizing condition (same logic as calc_baixa but for exclusion)
        fin_simple_cond = col("cod_mni_movimento").isin(self._exclusao_codes)
        rem_cond = fin_simple_cond  # start with simple union
        for rule in self.baixa_compl.get("rem", []):
            mni      = rule.get("cod_mni_movimento")
            tab_vals = rule.get("cod_mni_complemento_tabelado", [])
            if mni and tab_vals:
                rc = (col("cod_mni_movimento") == mni) & col("cod_mni_complemento_tabelado").isin(tab_vals)
                rem_cond = rem_cond | rc

        fin_events = fin_events.filter(rem_cond).select(
            col("id_processo").cast("string").alias("fin_id_processo"),
            col("dth_evento").alias("fin_dth_evento"),
        )

        # Keep only processes with NO finalizing event AFTER their latest distribution
        pending = (
            latest_dist.alias("ld").join(
                fin_events.alias("fm"),
                (col("ld.id_processo").cast("string") == col("fm.fin_id_processo"))
                & (col("fm.fin_dth_evento") > col("ld.dth_evento")),
                "left_anti",
            )
        )

        pending = pending.withColumn("cod_mag", lit(None).cast("string"))
        enriched = self._enrich(pending)

        # For emandamento: fallback cod_mag to cod_ult_relator
        enriched = enriched.withColumn(
            "cod_mag",
            coalesce(col("cod_mag"), col("cod_ult_relator")),
        )

        return self._to_output(enriched, "emandamento", dt_ref, ano_mes).dropDuplicates()

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def run(self, year_month: str) -> None:
        y, m   = int(year_month[:4]), int(year_month[5:])
        dt_ini = date(y, m, 1)
        dt_fim = _last_day_of_month(y, m)
        dt_ref = dt_fim

        logger.info("=" * 70)
        logger.info(
            "eproc2g_produtividade_emr  year_month=%s  %s → %s",
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
        logger.info("eproc2g_produtividade_emr  DONE  year_month=%s", year_month)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    today = date.today()
    default_ym = f"{today.year}_{today.month:02d}"
    p = argparse.ArgumentParser(
        description=(
            "EPROC2G Produtividade 6 indicators → "
            "Iceberg dw_2inst_ouro.tb_ft_eproc2g_produtividade"
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
    calc = Eproc2gProdutividadeCalculator(spark=spark, variaveis=variaveis)
    try:
        calc.run(args.year_month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
