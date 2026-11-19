#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pedido_vista_emr.py — EJUD Pedido de Vista → Iceberg (ouro layer)
==================================================================
Single-script design: no external .py dependencies.
All reads via spark.table() from the Glue catalog (glue_catalog.ejud.*).

Source tables (glue_catalog.ejud.*):
    movjulgparti, movjulg
    t_processo, tipoprocesso
    movimentofisico, movimentorelator

Target: glue_catalog.dw_2inst_ouro.tb_ft_ejud_pedido_vista
        Partitioned by (ano_mes)   format YYYY_MM

Indicators written per run:
    em_vista_periodo   vistas requested in [dt_ini, dt_fim] still unresolved at dt_fim
    em_vista_atual     vistas requested in [dt_ini, dt_fim] still unresolved today

Logic (both indicators share the same base):
    1. Filter movjulgparti: dthrmov in [dt_ini, dt_fim] AND codvoto = 9
    2. Keep latest vista event per coddoc (most recent dthrmov)
    3. left_anti join movjulg: any judgment AFTER vista dthrmov
       - em_vista_periodo: newer judgment must also be <= dt_fim
       - em_vista_atual:   newer judgment up to today (no upper bound)
    4. cod_mag  = magistrate who requested vista (movjulgparti.codmag)
    5. cod_local    = latest movimentofisico.codlocal  where dthrmov <= dth_vista
    6. cod_org_julg = latest movimentorelator.codorgjulg where dthrmov <= dth_vista

Write strategy (idempotent):
    DELETE FROM target WHERE ano_mes = 'YYYY_MM' AND indicador = '<name>'
    writeTo(...).append()

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_inspecao/scripts/pedido_vista_emr.py \\
      --dt-ini 2025-05-04 --dt-fim 2026-05-04 2>&1 | \\
      grep --line-buffered -E 'pedido_vista_emr|ERROR|Exception'

Validate in Athena:
    SELECT indicador, COUNT(*) AS n
    FROM dw_2inst_ouro.tb_ft_ejud_pedido_vista
    WHERE ano_mes = '2026_05'
    GROUP BY indicador ORDER BY indicador;
"""

import argparse
import logging
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    broadcast, col, concat, lit, lpad, regexp_replace,
    row_number, upper, when,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pedido_vista_emr")

# ── Catalog / Iceberg constants ───────────────────────────────────────────────
_CATALOG      = "glue_catalog"
_WAREHOUSE    = "s3://sgdai-data-lake"
_DB           = "ejud"
_TARGET_DB    = "dw_2inst_ouro"
_TARGET_TABLE = "tb_ft_ejud_pedido_vista"
_TARGET_LOC   = f"{_WAREHOUSE}/ouro/{_TARGET_DB}/{_TARGET_TABLE}"


# ── Utilities ─────────────────────────────────────────────────────────────────
def _norm(c):
    """Strip trailing .0 from numeric-as-string columns (e.g. '12.0' → '12')."""
    return regexp_replace(c.cast("string"), r"\.0$", "")


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudPedidoVista")
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
            cod_documento  STRING,
            cod_cnj        STRING,
            cod_classe     STRING,
            cod_sigilo     STRING,
            dth_vista      TIMESTAMP,
            cod_mag        STRING,
            cod_local      STRING,
            cod_org_julg   STRING,
            ind_originario STRING,
            indicador      STRING,
            dth_referencia TIMESTAMP,
            ano_mes        STRING
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
class PedidoVistaCalculator:
    """
    Calculates 2 EJUD pedido-de-vista indicators for a single inspection period.
    All reads via spark.table(glue_catalog.ejud.*).
    """

    def __init__(self, spark: SparkSession, db: str = _DB):
        self.spark = spark
        self.db    = db

    def _tbl(self, table: str) -> str:
        return f"{_CATALOG}.{self.db}.{table}"

    # ── Processo enrichment ───────────────────────────────────────────────────

    def _proc_df(self, coddoc_df: DataFrame) -> DataFrame:
        """
        Load t_processo semi-joined to coddoc_df.
        Join tipoprocesso for cod_classe (codextcnj).
        Build cod_cnj, cod_sigilo, ind_originario.

        Returns: coddoc, cod_cnj, cod_classe, cod_sigilo, ind_originario
        """
        coddoc_keys = coddoc_df.select(_norm(col("coddoc")).alias("coddoc")).distinct()

        tp = (
            self.spark.table(self._tbl("t_processo"))
            .withColumn("coddoc", _norm(col("coddoc")))
            .join(coddoc_keys, on="coddoc", how="semi")
        )

        tp_cols = [c.lower() for c in tp.columns]

        # CNJ number built from 6 fields
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

        tipo = (
            self.spark.table(self._tbl("tipoprocesso"))
            .select("codtipproc", _norm(col("codextcnj")).alias("codextcnj"))
        )

        return (
            tp
            .join(broadcast(tipo), on="codtipproc", how="left")
            .withColumn("cod_cnj",       num_proc)
            .withColumn("cod_sigilo",     _base)
            .withColumn("ind_originario",
                        when(num_proc.endswith("0000"), lit("S")).otherwise(lit("N")))
            .withColumn("cod_classe",     col("codextcnj"))
            .select("coddoc", "cod_cnj", "cod_classe", "cod_sigilo", "ind_originario")
        )

    # ── Location + org lookups (time-constrained to dth_vista) ───────────────

    def _latest_codlocal(self, vista_df: DataFrame) -> DataFrame:
        """
        Latest movimentofisico.codlocal per coddoc where dthrmov <= dth_vista.
        Prevents picking locations assigned after the vista judgment.

        vista_df must have: coddoc (normed string), dth_vista.
        Returns: coddoc, cod_local
        """
        mf = (
            self.spark.table(self._tbl("movimentofisico"))
            .withColumn("coddoc", _norm(col("coddoc")))
            .select(
                "coddoc",
                col("dthrmov").alias("mf_dthrmov"),
                _norm(col("codlocal")).alias("cod_local"),
            )
        )

        joined = (
            vista_df.select("coddoc", "dth_vista")
            .join(mf, on="coddoc", how="left")
            .filter(col("mf_dthrmov") <= col("dth_vista"))
        )

        w = Window.partitionBy("coddoc").orderBy(col("mf_dthrmov").desc())
        return (
            joined
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn", "dth_vista", "mf_dthrmov")
        )

    def _latest_codorgjulg(self, vista_df: DataFrame) -> DataFrame:
        """
        Latest movimentorelator.codorgjulg per coddoc where dthrmov <= dth_vista.
        Represents the responsible orgao at the time of the vista request.

        vista_df must have: coddoc (normed string), dth_vista.
        Returns: coddoc, cod_org_julg
        """
        mr = (
            self.spark.table(self._tbl("movimentorelator"))
            .withColumn("coddoc", _norm(col("coddoc")))
            .select(
                "coddoc",
                col("dthrmov").alias("mr_dthrmov"),
                _norm(col("codorgjulg")).alias("cod_org_julg"),
            )
        )

        joined = (
            vista_df.select("coddoc", "dth_vista")
            .join(mr, on="coddoc", how="left")
            .filter(col("mr_dthrmov") <= col("dth_vista"))
        )

        w = Window.partitionBy("coddoc").orderBy(col("mr_dthrmov").desc())
        return (
            joined
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn", "dth_vista", "mr_dthrmov")
        )

    # ── Vista base ────────────────────────────────────────────────────────────

    def _vista_base(self, dt_ini: date, dt_fim: date) -> DataFrame:
        """
        Common base: movjulgparti in [dt_ini, dt_fim] with codvoto=9.
        Keeps the latest vista event per coddoc.

        Returns: coddoc (normed string), dth_vista, cod_mag
        """
        parti = (
            self.spark.table(self._tbl("movjulgparti"))
            .filter(col("dthrmov") >= lit(dt_ini))
            .filter(col("dthrmov") <= lit(dt_fim))
            .filter(col("codvoto") == 9)
        )

        w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        return (
            parti
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select(
                _norm(col("coddoc")).alias("coddoc"),
                col("dthrmov").alias("dth_vista"),
                _norm(col("codmag")).alias("cod_mag"),
            )
        )

    def _exclude_resolved(
        self,
        vista_df: DataFrame,
        dt_upper: date = None,
    ) -> DataFrame:
        """
        Remove coddocs where a newer judgment in movjulg exists after dth_vista.
        dt_upper: upper bound for movjulg.dthrmov (None = no upper bound = today).
        """
        movjulg = (
            self.spark.table(self._tbl("movjulg"))
            .select(
                _norm(col("coddoc")).alias("jlg_coddoc"),
                col("dthrmov").alias("jlg_dthrmov"),
            )
        )
        if dt_upper is not None:
            movjulg = movjulg.filter(col("jlg_dthrmov") <= lit(dt_upper))

        return (
            vista_df.alias("v")
            .join(
                movjulg.alias("j"),
                (col("v.coddoc") == col("j.jlg_coddoc"))
                & (col("j.jlg_dthrmov") > col("v.dth_vista")),
                "left_anti",
            )
        )

    # ── Output projection ─────────────────────────────────────────────────────

    def _to_output(
        self,
        df: DataFrame,
        proc_df: DataFrame,
        indicador: str,
        dt_ref: date,
        ano_mes: str,
    ) -> DataFrame:
        """
        df must have: coddoc, dth_vista, cod_mag.
        Looks up cod_local and cod_org_julg constrained to dth_vista.
        Inner-joins proc_df; left-joins location and relator.
        """
        loc_df = self._latest_codlocal(df.select("coddoc", "dth_vista"))
        rel_df = self._latest_codorgjulg(df.select("coddoc", "dth_vista"))

        return (
            df
            .join(proc_df, on="coddoc", how="inner")
            .join(loc_df,  on="coddoc", how="left")
            .join(rel_df,  on="coddoc", how="left")
            .select(
                col("coddoc").alias("cod_documento"),
                col("cod_cnj"),
                col("cod_classe"),
                col("cod_sigilo"),
                col("dth_vista"),
                col("cod_mag"),
                col("cod_local"),
                col("cod_org_julg"),
                col("ind_originario"),
                lit(indicador).alias("indicador"),
                lit(dt_ref).cast("timestamp").alias("dth_referencia"),
                lit(ano_mes).alias("ano_mes"),
            )
        )

    # ── Indicators ────────────────────────────────────────────────────────────

    def calc_em_vista_periodo(
        self, dt_ini: date, dt_fim: date, ano_mes: str
    ) -> DataFrame:
        """
        Vistas requested in [dt_ini, dt_fim] with no judgment up to dt_fim after.
        Represents processes that ENDED the period in vista.
        """
        logger.info("calc_em_vista_periodo  %s → %s", dt_ini, dt_fim)
        vista = self._vista_base(dt_ini, dt_fim)
        vista = self._exclude_resolved(vista, dt_upper=dt_fim)
        proc  = self._proc_df(vista.select("coddoc").distinct())
        return self._to_output(vista, proc, "em_vista_periodo", dt_fim, ano_mes)

    def calc_em_vista_atual(
        self, dt_ini: date, dt_fim: date, ano_mes: str
    ) -> DataFrame:
        """
        Vistas requested in [dt_ini, dt_fim] with no judgment at all after (today).
        Represents processes STILL in vista at the time of this run.
        """
        logger.info("calc_em_vista_atual  %s → %s  (no upper bound on movjulg)", dt_ini, dt_fim)
        vista = self._vista_base(dt_ini, dt_fim)
        vista = self._exclude_resolved(vista, dt_upper=None)
        proc  = self._proc_df(vista.select("coddoc").distinct())
        return self._to_output(vista, proc, "em_vista_atual", date.today(), ano_mes)

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def run(self, dt_ini: date, dt_fim: date, ano_mes: str) -> None:
        logger.info("=" * 70)
        logger.info(
            "pedido_vista_emr  dt_ini=%s  dt_fim=%s  ano_mes=%s",
            dt_ini, dt_fim, ano_mes,
        )
        logger.info("=" * 70)

        indicators = [
            ("em_vista_periodo", self.calc_em_vista_periodo),
            ("em_vista_atual",   self.calc_em_vista_atual),
        ]

        for name, fn in indicators:
            logger.info("── %s ──", name)
            df = fn(dt_ini, dt_fim, ano_mes)
            _save_indicator(self.spark, df, name, ano_mes)

        logger.info("=" * 70)
        logger.info("pedido_vista_emr  DONE  ano_mes=%s", ano_mes)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=(
            "EJUD Pedido de Vista → "
            "Iceberg dw_2inst_ouro.tb_ft_ejud_pedido_vista"
        )
    )
    p.add_argument(
        "--dt-ini", required=True, metavar="YYYY-MM-DD",
        help="Start date of the inspection period (inclusive)",
    )
    p.add_argument(
        "--dt-fim", required=True, metavar="YYYY-MM-DD",
        help="End date of the inspection period (inclusive)",
    )
    p.add_argument(
        "--ano-mes", default=None, metavar="YYYY_MM",
        help=(
            "Partition key for the target table (default: derived from dt-fim, "
            "e.g. 2026-05-04 → 2026_05)"
        ),
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

    dt_ini = date.fromisoformat(args.dt_ini)
    dt_fim = date.fromisoformat(args.dt_fim)

    ano_mes = args.ano_mes or f"{dt_fim.year}_{dt_fim.month:02d}"

    global _TARGET_DB, _TARGET_LOC  # noqa: PLW0603
    _TARGET_DB  = args.target_db
    _TARGET_LOC = args.target_loc

    spark = _create_spark_session(
        args.executor_memory,
        args.executor_memory_overhead,
        args.shuffle_partitions,
    )
    calc = PedidoVistaCalculator(spark=spark)
    try:
        calc.run(dt_ini, dt_fim, ano_mes)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
