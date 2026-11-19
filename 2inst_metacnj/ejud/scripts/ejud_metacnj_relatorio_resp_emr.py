#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ejud_metacnj_relatorio_resp_emr.py — CNJ Meta reports by responsible magistrado/OJ

For each indicator event, attributes credit to the magistrado who performed the act:

  Judgment indicators  → magistrado from movjulg.codmagrel
                          fallback: movdespa.codmag
                          fallback: cod_prim_magistrado (from tb_ft_meta_aux)
  All other indicators → cod_prim_magistrado / cod_prim_oj (first distribution)

Judgment cod_indicadores (all metas combined):
  2  (julgadom01)    7  (julgadom02)    10 (julgadom02_ant)
  14 (julgadom04)    21 (julgadom06)    28 (julgadom08)
  35 (julgadom10)    42 (julgadom11)    49 (julgadom07)

Distribution cod_indicadores (safety cancel-check):
  1  (cnm01)     6  (dism02)     11 (dism02_ant)
  13 (dism04)    20 (dism06)     27 (dism08)
  34 (dism10)    41 (dism11)     48 (dism07)

Output schema (9 columns):
  meta_ano              INT
  <group>               BIGINT   cod_magistrado_resp  or  cod_oj_resp
  nm_meta               STRING
  segmento              STRING   '2inst' | 'turmas' | NULL
  n_julg                LONG
  n_denom               LONG
  pct                   DOUBLE
  dth_carga             TIMESTAMP

Sources:
  glue_catalog.dw_2inst_ouro.tb_ft_meta_analitico   — indicator events
  glue_catalog.dw_2inst_ouro.tb_ft_meta_aux         — cod_prim_magistrado / cod_prim_oj
  glue_catalog.ejud.movjulg                         — judgment magistrado (codmagrel)
  glue_catalog.ejud.movdespa                        — judgment magistrado fallback (codmag)
  glue_catalog.ejud.movcancedistr                   — cancelled distributions (safety filter)

Targets:
  glue_catalog.dw_2inst_ouro.tb_ft_meta_por_mag_resp
  glue_catalog.dw_2inst_ouro.tb_ft_meta_por_oj_resp

Join strategy for movjulg/movdespa:
  Join key: (id_processo = coddoc AND date(dth_evento_indicador) = date(dthrmov))
  Dedup: one row per (coddoc, date) — keep latest dthrmov per day to avoid fan-out.

Cancel check (movcancedistr):
  Column name: dthrmovdistrcance (not dthrmovdistrcanc)

Usage:
    spark-submit --master yarn --deploy-mode client \\
      --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \\
      s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts/ejud_metacnj_relatorio_resp_emr.py \\
      --year 2026 2>&1 | \\
      grep --line-buffered -E 'ejud_metacnj_relatorio_resp|ERROR|Exception'
"""

import argparse
import logging
import sys
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    coalesce, col, current_timestamp, lit, round as _round,
    row_number, sum as _sum, to_date, when,
)
from pyspark.sql.types import LongType

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ejud_metacnj_relatorio_resp_emr")

# ── Infra ─────────────────────────────────────────────────────────────────────
_CATALOG    = "glue_catalog"
_WAREHOUSE  = "s3://sgdai-data-lake"
_DB_OURO    = "dw_2inst_ouro"
_DB_BRONZE  = "ejud"
_SOURCE     = "tb_ft_meta_analitico"
_AUX        = "tb_ft_meta_aux"
_TARGET_MAG = "tb_ft_meta_por_mag_resp"
_TARGET_OJ  = "tb_ft_meta_por_oj_resp"

# ── Indicator sets ────────────────────────────────────────────────────────────
# All julgado indicators across every meta — these trigger the bronze join.
_JULG_INDS = {2, 7, 10, 14, 21, 28, 35, 42, 49}

# Distribution indicators — checked against movcancedistr (safety filter).
_DIST_INDS = {1, 6, 11, 13, 20, 27, 34, 41, 48}

# ── Competência ───────────────────────────────────────────────────────────────
TURMAS    = [10, 11, 14, 15, 19, 20, 22]
NOT_TO    = [1, 2, 9, 12, 21]

# ── cod_indicador ─────────────────────────────────────────────────────────────
I_CNM01 = 1;  I_JULG01 = 2;  I_SUS01 = 3;  I_DES01 = 4
I_DISM02 = 6; I_JULG02 = 7;  I_SUS02 = 8
I_DES02_ANT = 9; I_JULG02_ANT = 10; I_DISM02_ANT = 11; I_SUS02_ANT = 12
I_DISM04 = 13; I_JULG04 = 14; I_SUS04 = 15
I_DISM06 = 20; I_JULG06 = 21; I_SUS06 = 22
I_DISM07 = 48; I_JULG07 = 49; I_SUS07 = 50
I_DISM08 = 27; I_JULG08 = 28; I_SUS08 = 29
I_DISM10 = 34; I_JULG10 = 35; I_SUS10 = 36
I_DISM11 = 41; I_JULG11 = 42; I_SUS11 = 43

# ── Meta 07 assunto codes ─────────────────────────────────────────────────────
M07_INDIGENA   = [9989, 12824, 13199, 9901, 15220, 10102, 3647, 15114, 10105,
                  10104, 10103, 15595, 15614, 15609, 15592, 15597, 15610, 15604,
                  15615, 15611]
M07_QUILOMBOLA = [12031, 12825, 15618, 15613]
M07_RACISMO    = [3613, 9873, 11849, 11844, 12543, 12861, 14101, 14102, 14100,
                  15134, 15135, 12547, 14111, 14110, 14108, 14697, 14699, 15127,
                  15128]

# ── Meta 08 assunto codes ─────────────────────────────────────────────────────
FEM_ASSUNTOS   = [12091, 12358]
VD_ASSUNTOS    = [10948, 10949, 11979, 12194, 12196, 14097, 14098, 14226, 14227,
                  14228, 14229, 14943, 14945, 14942, 14944, 15511]
VD_CLASSES_CNJ = [1268, 15309, 12423]


# ── SparkSession ──────────────────────────────────────────────────────────────
def _create_spark_session(executor_memory="8g", executor_memory_overhead="4g",
                           shuffle_partitions=200) -> SparkSession:
    return (
        SparkSession.builder
        .appName("EjudMetaCNJ_RelatorioResp")
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
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


# ── Iceberg helpers ───────────────────────────────────────────────────────────
def _ensure_iceberg_table(spark: SparkSession, fqn: str, location: str,
                           df: DataFrame) -> None:
    type_map = {
        "string": "STRING", "long": "BIGINT", "integer": "INT",
        "double": "DOUBLE", "timestamp": "TIMESTAMP", "timestamp_ntz": "TIMESTAMP",
    }
    col_defs = []
    for f in df.schema.fields:
        sql_t = type_map.get(str(f.dataType.typeName()).lower(), "STRING")
        col_defs.append(f"  `{f.name}` {sql_t}")
    non_part = [d for d in col_defs if not d.strip().startswith("`meta_ano`")]
    part_def = [d for d in col_defs if d.strip().startswith("`meta_ano`")]
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        + ",\n".join(non_part + part_def)
        + "\n)\nUSING iceberg\nPARTITIONED BY (meta_ano)\n"
        + f"LOCATION '{location}'\n"
        + "TBLPROPERTIES (\n"
        + "  'format-version' = '2',\n"
        + "  'write.parquet.compression-codec' = 'zstd'\n)"
    )
    spark.sql(ddl)


def _save(df: DataFrame, fqn: str, meta_ano: int) -> None:
    logger.info("Deleting meta_ano=%s → %s", meta_ano, fqn)
    spark = df.sparkSession
    spark.sql(f"DELETE FROM {fqn} WHERE meta_ano = {meta_ano}")
    logger.info("Writing meta_ano=%s → %s", meta_ano, fqn)
    df.writeTo(fqn).option("write-audit-publish", "false").append()


# ── Slice builder ─────────────────────────────────────────────────────────────

def _cnt(ind: int, extra=None):
    c = col("cod_indicador") == ind
    if extra is not None:
        c = c & extra
    return _sum(when(c, 1).otherwise(0))


def _build_slice(
    df: DataFrame,
    group_col: str,
    nm_meta: str,
    segmento: Optional[str],
    ind_julg: int,
    ind_dism: int,
    ind_sus: int,
    multiplier: float,
    ind_des: Optional[int] = None,
    ind_cnm_add: Optional[int] = None,
    row_filter=None,
) -> DataFrame:
    all_inds = [ind_julg, ind_dism, ind_sus]
    if ind_des     is not None: all_inds.append(ind_des)
    if ind_cnm_add is not None: all_inds.append(ind_cnm_add)

    agg_exprs = [
        _cnt(ind_julg,    row_filter).alias("n_julg"),
        _cnt(ind_dism,    row_filter).alias("n_dism"),
        _cnt(ind_sus,     row_filter).alias("n_sus"),
    ]
    if ind_des     is not None:
        agg_exprs.append(_cnt(ind_des,     row_filter).alias("n_des"))
    if ind_cnm_add is not None:
        agg_exprs.append(_cnt(ind_cnm_add, row_filter).alias("n_cnm_add"))

    agg = (
        df.filter(col("cod_indicador").isin(all_inds))
        .groupBy(group_col, "meta_ano")
        .agg(*agg_exprs)
    )

    if ind_cnm_add is not None:
        denom_expr = col("n_dism") + col("n_cnm_add") - col("n_sus")
    elif ind_des is not None:
        denom_expr = col("n_dism") - col("n_sus") - col("n_des")
    else:
        denom_expr = col("n_dism") - col("n_sus")

    return (
        agg
        .withColumn("n_denom", denom_expr)
        .withColumn("pct",
            when(col("n_denom") > 0,
                 _round(col("n_julg") / col("n_denom") * multiplier, 2))
            .otherwise(lit(None).cast("double"))
        )
        .withColumn("nm_meta",  lit(nm_meta))
        .withColumn("segmento", lit(segmento))
        .select(
            col(group_col),
            col("meta_ano"),
            col("nm_meta"),
            col("segmento"),
            col("n_julg").cast(LongType()),
            col("n_denom").cast(LongType()),
            col("pct"),
        )
    )


# ── Report builder ────────────────────────────────────────────────────────────

class MetaRespReportBuilder:

    def __init__(self, spark: SparkSession, catalog: str = _CATALOG,
                 db_ouro: str = _DB_OURO, db_bronze: str = _DB_BRONZE):
        self.spark     = spark
        self.catalog   = catalog
        self.db_ouro   = db_ouro
        self.db_bronze = db_bronze

    def _fqn(self, db: str, table: str) -> str:
        return f"{self.catalog}.{db}.{table}"

    def _loc(self, table: str) -> str:
        return f"{_WAREHOUSE}/ouro/{self.db_ouro}/{table}"

    def _dedup_mov(self, table: str, mag_col: str, mag_alias: str) -> DataFrame:
        """
        Read a bronze movement table (movjulg or movdespa), dedup to one row per
        (coddoc, date(dthrmov)) keeping latest dthrmov per day.
        Returns: id_processo (STRING), dt (DATE), <mag_alias> (LONG)
        Note: codorgjulg not present in movjulg/movdespa — OJ enriched separately
              via movimentorelator in _movrel_oj_lookup().
        """
        w = Window.partitionBy("coddoc", "dt").orderBy(col("dthrmov").desc())
        return (
            self.spark.table(self._fqn(self.db_bronze, table))
            .select(
                col("coddoc"),
                col("dthrmov"),
                col(mag_col).cast(LongType()).alias(mag_alias),
            )
            .withColumn("dt", to_date(col("dthrmov")))
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn", "dthrmov")
            .withColumn("id_processo", col("coddoc").cast("string"))
            .drop("coddoc")
        )

    def _mag_any_lookup(self, proc_ids: DataFrame) -> DataFrame:
        """
        Fallback magistrado lookup: latest movjulg.codmagrel per coddoc, no date filter.
        Used when same-date join on _dedup_mov misses (date in analitico differs from
        movjulg.dthrmov — common when M01 ETL derived dth_evento_indicador from a
        different movement table, e.g. movacordao/movimento).
        Returns: id_processo (STRING), _mag_julg_any (LONG)
        """
        return (
            self.spark.table(self._fqn(self.db_bronze, "movjulg"))
            .join(proc_ids, on=col("coddoc").cast("string") == col("id_processo"), how="semi")
            .select(
                col("coddoc"),
                col("dthrmov"),
                col("codmagrel").cast(LongType()).alias("_mag_julg_any"),
            )
            .filter(col("_mag_julg_any").isNotNull())
            .withColumn(
                "_rn",
                row_number().over(
                    Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
                )
            )
            .filter(col("_rn") == 1)
            .drop("_rn", "dthrmov")
            .withColumn("id_processo", col("coddoc").cast("string"))
            .drop("coddoc")
        )

    def _movrel_oj_lookup(self, proc_ids: DataFrame) -> DataFrame:
        """
        Build OJ lookup from movimentorelator for judgment attribution.
        For each (coddoc, codmagrel) pair keep the latest dthrmov entry → codorgjulg.
        After computing cod_magistrado_resp, join this on
        (id_processo = coddoc AND cod_magistrado_resp = codmagrel)
        to get the OJ where that magistrado was last assigned for that process.
        Returns: id_processo (STRING), _movrel_codmagrel (LONG), _movrel_oj (LONG)
        """
        return (
            self.spark.table(self._fqn(self.db_bronze, "movimentorelator"))
            .join(proc_ids, on=col("coddoc").cast("string") == col("id_processo"), how="semi")
            .select(
                col("coddoc"),
                col("dthrmov"),
                col("codmagrel").cast(LongType()).alias("_movrel_codmagrel"),
                col("codorgjulg").cast(LongType()).alias("_movrel_oj"),
            )
            .withColumn(
                "_rn",
                row_number().over(
                    Window.partitionBy("coddoc", "_movrel_codmagrel").orderBy(col("dthrmov").desc())
                )
            )
            .filter(col("_rn") == 1)
            .drop("_rn", "dthrmov")
            .withColumn("id_processo", col("coddoc").cast("string"))
            .drop("coddoc")
        )

    def _movrel_oj_any(self, proc_ids: DataFrame) -> DataFrame:
        """
        Fallback OJ lookup: latest codorgjulg per coddoc, ignoring codmagrel.
        Used when magistrado-specific lookup (_movrel_oj_lookup) returns no OJ.
        Returns: id_processo (STRING), _movrel_oj_any (LONG)
        """
        return (
            self.spark.table(self._fqn(self.db_bronze, "movimentorelator"))
            .join(proc_ids, on=col("coddoc").cast("string") == col("id_processo"), how="semi")
            .select(
                col("coddoc"),
                col("dthrmov"),
                col("codorgjulg").cast(LongType()).alias("_movrel_oj_any"),
            )
            .filter(col("_movrel_oj_any").isNotNull())
            .withColumn(
                "_rn",
                row_number().over(
                    Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
                )
            )
            .filter(col("_rn") == 1)
            .drop("_rn", "dthrmov")
            .withColumn("id_processo", col("coddoc").cast("string"))
            .drop("coddoc")
        )

    def _source(self, meta_ano: int) -> DataFrame:
        """
        Build enriched source DataFrame for meta_ano:
          1. analitico + aux (prim columns)
          2. safety join: drop distribution rows cancelled in movcancedistr
             (movcancedistr.coddoc = movdistribuicao.coddoc AND
              movcancedistr.dthrmovdistrcance = movdistribuicao.dthrmov)
          3. dedup movjulg / movdespa on (coddoc, date) → judgment magistrado
          4. join deduped tables on (id_processo, date(dth_evento_indicador))
          5. compute cod_magistrado_resp = coalesce(movjulg.codmagrel, movdespa.codmag, prim)
          6. enrich OJ: movimentorelator latest per (coddoc, codmagrel=cod_magistrado_resp)
          7. compute cod_oj_resp = coalesce(movrel.codorgjulg, prim) for judgment rows
        """
        spark = self.spark

        # ── 1. Base: analitico + aux ───────────────────────────────────────────
        analitico = (
            spark.table(self._fqn(self.db_ouro, _SOURCE))
            .filter(col("meta_ano") == lit(meta_ano))
            .withColumn("dt_evento", to_date(col("dth_evento_indicador")))
        )
        aux = (
            spark.table(self._fqn(self.db_ouro, _AUX))
            .filter(col("meta_ano") == lit(meta_ano))
            .select(
                col("id_processo"),
                col("cod_prim_magistrado").cast(LongType()).alias("_prim_mag"),
                col("cod_prim_oj").cast(LongType()).alias("_prim_oj"),
            )
        )
        base = analitico.join(aux, on="id_processo", how="left")

        # ── 2. Safety: exclude cancelled distributions ─────────────────────────
        cancelled = (
            spark.table(self._fqn(self.db_bronze, "movcancedistr"))
            .select(
                col("coddoc").cast("string").alias("_c_proc"),
                to_date(col("dthrmovdistrcance")).alias("_c_dt"),
            )
            .distinct()
        )
        base = (
            base
            .join(
                cancelled,
                (col("id_processo") == col("_c_proc"))
                & (col("dt_evento") == col("_c_dt")),
                "left",
            )
            .filter(
                # Keep: not a distribution row, OR distribution row that was NOT cancelled
                ~col("cod_indicador").isin(list(_DIST_INDS))
                | col("_c_proc").isNull()
            )
            .drop("_c_proc", "_c_dt")
        )

        # ── 3. Process IDs for push-down semi-join (only judgment rows need bronze) ─
        proc_ids = (
            base.filter(col("cod_indicador").isin(list(_JULG_INDS)))
            .select("id_processo").distinct()
        )

        movjulg_dd  = self._dedup_mov("movjulg",  "codmagrel", "_mag_julg")
        movdespa_dd = self._dedup_mov("movdespa",  "codmag",    "_mag_despa")
        mag_any     = self._mag_any_lookup(proc_ids)

        # Restrict bronze reads to only relevant processes
        movjulg_dd  = movjulg_dd.join(proc_ids,  on="id_processo", how="semi")
        movdespa_dd = movdespa_dd.join(proc_ids,  on="id_processo", how="semi")

        # ── 4. Join judgment magistrado enrichment ─────────────────────────────
        # Rename id_processo on right side to avoid ambiguous column reference
        movjulg_dd = movjulg_dd.withColumnRenamed("id_processo", "_jd_proc")
        base = (
            base
            .join(
                movjulg_dd.withColumnRenamed("dt", "_dt_julg"),
                (col("id_processo") == col("_jd_proc")) & (col("dt_evento") == col("_dt_julg")),
                "left",
            )
            .drop("_jd_proc", "_dt_julg")
        )
        movdespa_dd = movdespa_dd.withColumnRenamed("id_processo", "_dd_proc")
        base = (
            base
            .join(
                movdespa_dd.withColumnRenamed("dt", "_dt_despa"),
                (col("id_processo") == col("_dd_proc")) & (col("dt_evento") == col("_dt_despa")),
                "left",
            )
            .drop("_dd_proc", "_dt_despa")
        )
        # Fallback: latest movjulg entry (no date constraint) for date-mismatch cases
        mag_any = mag_any.withColumnRenamed("id_processo", "_ja_proc")
        base = (
            base
            .join(mag_any, col("id_processo") == col("_ja_proc"), "left")
            .drop("_ja_proc")
        )

        # ── 5. Compute cod_magistrado_resp ─────────────────────────────────────
        is_julg = col("cod_indicador").isin(list(_JULG_INDS))

        base = base.withColumn(
            "cod_magistrado_resp",
            when(is_julg,
                 coalesce(col("_mag_julg"), col("_mag_despa"), col("_mag_julg_any"), col("_prim_mag")))
            .otherwise(col("_prim_mag"))
            .cast(LongType()),
        ).drop("_mag_julg", "_mag_despa", "_mag_julg_any")

        # ── 6. Enrich OJ via movimentorelator (codorgjulg absent in movjulg/movdespa) ─
        # Primary: latest movimentorelator entry for (coddoc, codmagrel=cod_magistrado_resp).
        # Fallback: latest ANY movimentorelator entry for coddoc (codorgjulg = court unit,
        #           not magistrado-specific — safe to use even when magistrado changed).
        movrel_oj = self._movrel_oj_lookup(proc_ids)
        movrel_oj = movrel_oj.withColumnRenamed("id_processo", "_mr_proc")
        base = (
            base
            .join(
                movrel_oj,
                (col("id_processo") == col("_mr_proc"))
                & (col("cod_magistrado_resp") == col("_movrel_codmagrel")),
                "left",
            )
            .drop("_mr_proc")
        )

        movrel_any = self._movrel_oj_any(proc_ids)
        movrel_any = movrel_any.withColumnRenamed("id_processo", "_ma_proc")
        base = (
            base
            .join(
                movrel_any,
                col("id_processo") == col("_ma_proc"),
                "left",
            )
            .drop("_ma_proc")
        )

        # ── 7. Compute cod_oj_resp ─────────────────────────────────────────────
        base = (
            base
            .withColumn(
                "cod_oj_resp",
                when(is_julg,
                     coalesce(col("_movrel_oj"), col("_movrel_oj_any"), col("_prim_oj")))
                .otherwise(coalesce(col("_movrel_oj_any"), col("_prim_oj")))
                .cast(LongType()),
            )
            .drop("_prim_mag", "_prim_oj",
                  "_movrel_codmagrel", "_movrel_oj", "_movrel_oj_any",
                  "dt_evento")
        )

        return base

    def build(self, df: DataFrame, group_col: str) -> DataFrame:
        is_2inst  = ~col("cod_competencia").isin(TURMAS + NOT_TO)
        is_turmas = col("cod_competencia").isin(TURMAS)

        is_ind  = col("id_assunto_principal").isin(M07_INDIGENA)
        is_quil = col("id_assunto_principal").isin(M07_QUILOMBOLA)
        is_rac  = col("id_assunto_principal").isin(M07_RACISMO)

        is_fem  = col("id_assunto_principal").isin(FEM_ASSUNTOS)
        is_vd   = (
            (col("id_assunto_principal").isin(VD_ASSUNTOS)
             | col("cod_classe_cnj").isin(VD_CLASSES_CNJ))
            & ~is_fem
        )

        slices: List[DataFrame] = [
            _build_slice(df, group_col, "M01",  "2inst",
                         ind_julg=I_JULG01, ind_dism=I_CNM01, ind_sus=I_SUS01,
                         multiplier=100.0, ind_cnm_add=I_DES01,
                         row_filter=is_2inst),
            _build_slice(df, group_col, "M01",  "turmas",
                         ind_julg=I_JULG01, ind_dism=I_CNM01, ind_sus=I_SUS01,
                         multiplier=100.0, ind_cnm_add=I_DES01,
                         row_filter=is_turmas),
            _build_slice(df, group_col, "M02",  "2inst",
                         ind_julg=I_JULG02, ind_dism=I_DISM02, ind_sus=I_SUS02,
                         multiplier=1000.0 / 9.0, row_filter=is_2inst),
            _build_slice(df, group_col, "M02",  "turmas",
                         ind_julg=I_JULG02, ind_dism=I_DISM02, ind_sus=I_SUS02,
                         multiplier=1000.0 / 9.5, row_filter=is_turmas),
            _build_slice(df, group_col, "M02d", "2inst",
                         ind_julg=I_JULG02_ANT, ind_dism=I_DISM02_ANT,
                         ind_sus=I_SUS02_ANT, multiplier=100.0,
                         ind_des=I_DES02_ANT),
            _build_slice(df, group_col, "M04",  "2inst",
                         ind_julg=I_JULG04, ind_dism=I_DISM04, ind_sus=I_SUS04,
                         multiplier=100.0, row_filter=is_2inst),
            _build_slice(df, group_col, "M06",  "2inst",
                         ind_julg=I_JULG06, ind_dism=I_DISM06, ind_sus=I_SUS06,
                         multiplier=1000.0 / 5.0, row_filter=is_2inst),
            _build_slice(df, group_col, "M07a", "2inst",
                         ind_julg=I_JULG07, ind_dism=I_DISM07, ind_sus=I_SUS07,
                         multiplier=1000.0 / 5.0, row_filter=is_ind & is_2inst),
            _build_slice(df, group_col, "M07b", "2inst",
                         ind_julg=I_JULG07, ind_dism=I_DISM07, ind_sus=I_SUS07,
                         multiplier=1000.0 / 5.0, row_filter=is_quil & is_2inst),
            _build_slice(df, group_col, "M07c", "2inst",
                         ind_julg=I_JULG07, ind_dism=I_DISM07, ind_sus=I_SUS07,
                         multiplier=1000.0 / 5.0, row_filter=is_rac & is_2inst),
            _build_slice(df, group_col, "M08a", "2inst",
                         ind_julg=I_JULG08, ind_dism=I_DISM08, ind_sus=I_SUS08,
                         multiplier=1000.0 / 9.0, row_filter=is_vd & is_2inst),
            _build_slice(df, group_col, "M08b", "2inst",
                         ind_julg=I_JULG08, ind_dism=I_DISM08, ind_sus=I_SUS08,
                         multiplier=1000.0 / 7.5, row_filter=is_fem & is_2inst),
            _build_slice(df, group_col, "M10",  "2inst",
                         ind_julg=I_JULG10, ind_dism=I_DISM10, ind_sus=I_SUS10,
                         multiplier=1000.0 / 9.9, row_filter=is_2inst),
            _build_slice(df, group_col, "M11",  "2inst",
                         ind_julg=I_JULG11, ind_dism=I_DISM11, ind_sus=I_SUS11,
                         multiplier=1000.0 / 9.9, row_filter=is_2inst),
        ]

        result = slices[0]
        for s in slices[1:]:
            result = result.unionByName(s)

        result = result.filter((col("n_julg") > 0) | (col("n_denom") > 0))
        return result.withColumn("dth_carga", current_timestamp())

    def _discover_years(self) -> List[int]:
        rows = (
            self.spark.table(self._fqn(self.db_ouro, _SOURCE))
            .select("meta_ano").distinct()
            .orderBy("meta_ano")
            .collect()
        )
        return [r.meta_ano for r in rows]

    def _init_tables(self, sample_mag: DataFrame, sample_oj: DataFrame) -> None:
        for fqn, loc, sample in [
            (self._fqn(self.db_ouro, _TARGET_MAG), self._loc(_TARGET_MAG), sample_mag),
            (self._fqn(self.db_ouro, _TARGET_OJ),  self._loc(_TARGET_OJ),  sample_oj),
        ]:
            _ensure_iceberg_table(self.spark, fqn, loc, sample)

    def run(self, years: Optional[List[int]] = None) -> None:
        if years is None:
            years = self._discover_years()
            logger.info("No --year supplied; processing all years found: %s", years)

        logger.info("=" * 70)
        logger.info("ejud_metacnj_relatorio_resp_emr  years=%s", years)
        logger.info("=" * 70)

        tables_ready = False

        for meta_ano in years:
            logger.info("── meta_ano=%s ──", meta_ano)
            src = self._source(meta_ano).cache()
            rpt_mag = self.build(src, "cod_magistrado_resp")
            rpt_oj  = self.build(src, "cod_oj_resp")

            if not tables_ready:
                self._init_tables(rpt_mag, rpt_oj)
                tables_ready = True

            _save(rpt_mag, self._fqn(self.db_ouro, _TARGET_MAG), meta_ano)
            _save(rpt_oj,  self._fqn(self.db_ouro, _TARGET_OJ),  meta_ano)
            src.unpersist()
            logger.info("meta_ano=%s written", meta_ano)

        logger.info("=" * 70)
        logger.info("ejud_metacnj_relatorio_resp_emr  DONE  years=%s", years)
        logger.info("=" * 70)


# ── Argparse + main ───────────────────────────────────────────────────────────
def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Build CNJ Meta reports by responsible magistrado/OJ at event time"
    )
    p.add_argument(
        "--year", type=int, default=None,
        help="Single meta_ano to process. Omit to process ALL years.",
    )
    p.add_argument("--catalog",                  default=_CATALOG)
    p.add_argument("--db-ouro",                  default=_DB_OURO)
    p.add_argument("--db-bronze",                default=_DB_BRONZE)
    p.add_argument("--executor-memory",          default="8g")
    p.add_argument("--executor-memory-overhead", default="4g")
    p.add_argument("--shuffle-partitions", type=int, default=200)
    return p.parse_args(argv)


def main(argv=None):
    args  = _parse_args(argv)
    spark = _create_spark_session(
        executor_memory=args.executor_memory,
        executor_memory_overhead=args.executor_memory_overhead,
        shuffle_partitions=args.shuffle_partitions,
    )
    builder = MetaRespReportBuilder(
        spark=spark, catalog=args.catalog,
        db_ouro=args.db_ouro, db_bronze=args.db_bronze,
    )
    years = [args.year] if args.year is not None else None
    try:
        builder.run(years=years)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
