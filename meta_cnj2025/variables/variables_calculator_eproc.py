"""
EPROC Meta Variables Calculator

This module provides variable calculation functionality for EPROC data,
similar to variables_calculator.py for EJUD.

Key differences from EJUD:
- Uses id_processo (string) instead of coddoc (long)
- Uses dth_inicio/dth_fim instead of dthrmov/dth_fim
- Uses year_month partitioned situacao data
- Uses different table structures from bronze layer

Variables:
- cnm: Distribution (new cases in current year)
- primeirasentm: First sentences
- baixm: Archived/closed processes
- mpum: Protective measures and decisions
- julgadom: Judged processes (composite)
- decm: Decisions in month
- susm: Suspended processes
- desm: Unsuspended processes
"""

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import os
import logging
import gc

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, year, month, desc, asc, current_timestamp,
        row_number, when
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        TimestampType, DateType, LongType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from utils.parquet_utils import meta_export_path, eproc_situacao_base


class MetaVariablesEproc:
    """
    EPROC Meta Variables Calculator.

    Provides methods for calculating meta variables for EPROC data sources.
    Uses loading_mode to support different filtering patterns:
    - "dism_filter": Filter by dism parquet (for metas 02+)
    - "eligible_processes": Filter by eligible_processes_df directly (for meta 01)
    """

    def __init__(self, spark, config_manager, logger, misc, eligible_processes_df, meta_logger,
                 period_start, period_end, meta_cutoff_date, meta_15year_cutoff, meta_number,
                 situacao_base_path: str = None, loading_mode: str = "eligible_processes"):
        """
        Initialize EPROC Meta Variables Calculator.

        Args:
            spark: SparkSession
            config_manager: MetaConfigManager instance
            logger: Logger instance
            misc: MetaMiscellaneous instance
            eligible_processes_df: DataFrame with eligible id_processo
            meta_logger: MetaLogger instance
            period_start: Start of calculation period
            period_end: End of calculation period
            meta_cutoff_date: Cutoff date for meta calculations
            meta_15year_cutoff: 15-year cutoff for historical calculations
            meta_number: Meta number (e.g., "01")
            situacao_base_path: Base path for situacao data
            loading_mode: One of "dism_filter" or "eligible_processes"
        """
        self.spark = spark
        self.config_manager = config_manager
        self.logger = logger
        self.misc = misc
        self.eligible_processes_df = eligible_processes_df
        self.meta_logger = meta_logger
        self.period_start = period_start
        self.period_end = period_end
        self.meta_cutoff_date = meta_cutoff_date
        self.meta_15year_cutoff = meta_15year_cutoff
        self.meta_number = meta_number
        self.loading_mode = loading_mode

        # Set situacao base path
        self.situacao_base_path = situacao_base_path or eproc_situacao_base()

        # Empty schema for fallback
        self.empty_schema = StructType([
            StructField("id_processo", StringType(), True),
            StructField("dth_inicio", TimestampType(), True),
            StructField("dth_fim", TimestampType(), True),
            StructField("situacao", IntegerType(), True),
            StructField("variavel", StringType(), True)
        ])

    def _create_empty_result_df(self) -> DataFrame:
        """Create an empty DataFrame with standard result schema."""
        return self.spark.createDataFrame([], self.empty_schema)

    def load_situacao_unified(self, situacao: int,
                               apply_period_filter: bool = False,
                               period_start: Optional[date] = None,
                               period_end: Optional[date] = None) -> DataFrame:
        """
        Unified situacao loading that respects the configured loading_mode.

        Args:
            situacao: Situacao code to load
            apply_period_filter: Whether to apply period filtering
            period_start: Start date for period filtering
            period_end: End date for period filtering

        Returns:
            Spark DataFrame with filtered situacao data
        """
        if self.loading_mode == "dism_filter":
            return self._load_situacao_with_dism_filter(
                situacao, apply_period_filter, period_start, period_end
            )
        else:  # eligible_processes
            return self._load_situacao_with_eligible_filter(
                situacao, apply_period_filter, period_start, period_end
            )

    def _load_situacao_with_eligible_filter(self, situacao: int,
                                             apply_period_filter: bool = False,
                                             period_start: Optional[date] = None,
                                             period_end: Optional[date] = None) -> DataFrame:
        """
        Load situacao filtered by eligible_processes_df directly (meta_01 pattern).
        """
        from datetime import datetime

        self.logger.info(f"📂 Loading situacao {situacao} with eligible_processes filter")

        if self.eligible_processes_df is None:
            self.logger.warning("No eligible processes available")
            return self._create_empty_result_df()

        # Determine date range
        start_date = period_start or self.meta_cutoff_date or self.period_start
        end_date = period_end or self.period_end

        # Build month range
        try:
            cur = start_date.replace(day=1)
            months = []
            while cur <= end_date:
                months.append((cur.year, cur.month))
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1)
                else:
                    cur = cur.replace(month=cur.month + 1)
        except Exception as e:
            self.logger.warning(f"⚠️ Could not determine month range: {e}")
            return self._create_empty_result_df()

        month_dfs = []

        for (y, m) in months:
            ym_dir = f"{self.situacao_base_path}/year_month={y}-{m:02d}"

            try:
                if not os.path.exists(ym_dir):
                    continue

                # Read month data
                df_month = self.spark.read.option("recursiveFileLookup", "true").parquet(ym_dir)

                # Filter by situacao
                if "situacao" in df_month.columns:
                    df_filtered = df_month.filter(col("situacao") == situacao)
                else:
                    continue

                # Apply date filter if requested
                if apply_period_filter and "dth_inicio" in df_filtered.columns:
                    start_dt = datetime.combine(start_date, datetime.min.time())
                    end_dt = datetime.combine(end_date, datetime.max.time())
                    df_filtered = df_filtered.filter(
                        (col("dth_inicio") >= start_dt) & (col("dth_inicio") <= end_dt)
                    )

                # Check for data
                try:
                    has_rows = len(df_filtered.limit(1).collect()) > 0
                except Exception:
                    has_rows = False

                if has_rows:
                    month_dfs.append(df_filtered)

            except Exception as e:
                self.logger.debug(f"ℹ️ Could not read month {ym_dir}: {e}")
                continue

        if not month_dfs:
            self.logger.warning(f"❌ No data found for situacao {situacao}")
            return self._create_empty_result_df()

        # Union all months
        df = month_dfs[0]
        for d in month_dfs[1:]:
            df = df.unionByName(d, allowMissingColumns=True)

        # Filter by eligible processes
        eligible_str = self.eligible_processes_df.select("id_processo").distinct()
        df = df.withColumn("id_str", col("id_processo").cast(StringType()))
        eligible_str = eligible_str.withColumn("id_str", col("id_processo").cast(StringType()))
        df = df.join(eligible_str.select("id_str"), on="id_str", how="inner").drop("id_str")

        self.logger.info(f"✅ Successfully loaded situacao {situacao}")
        return df

    def _load_situacao_with_dism_filter(self, situacao: int,
                                         apply_period_filter: bool = False,
                                         period_start: Optional[date] = None,
                                         period_end: Optional[date] = None) -> DataFrame:
        """
        Load situacao filtered by dism parquet (meta_02+ pattern).
        """
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "eproc2g")
        dism_path = f"{_meta_export_path}/dism{self.meta_number}.parquet"

        try:
            dism_df = self.spark.read.parquet(dism_path).select("id_processo").distinct()
        except Exception as e:
            self.logger.error(f"❌ Failed to load dism from {dism_path}: {e}")
            return self._create_empty_result_df()

        # Use the same loading logic but with dism as the filter
        self.logger.info(f"📂 Loading situacao {situacao} with dism filter")

        start_date = period_start or self.meta_cutoff_date or self.period_start
        end_date = period_end or self.period_end

        # Build month range
        try:
            cur = start_date.replace(day=1)
            months = []
            while cur <= end_date:
                months.append((cur.year, cur.month))
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1)
                else:
                    cur = cur.replace(month=cur.month + 1)
        except Exception as e:
            self.logger.warning(f"⚠️ Could not determine month range: {e}")
            return self._create_empty_result_df()

        month_dfs = []

        for (y, m) in months:
            ym_dir = f"{self.situacao_base_path}/year_month={y}-{m:02d}"

            try:
                if not os.path.exists(ym_dir):
                    continue

                df_month = self.spark.read.option("recursiveFileLookup", "true").parquet(ym_dir)

                if "situacao" in df_month.columns:
                    df_filtered = df_month.filter(col("situacao") == situacao)
                else:
                    continue

                if apply_period_filter and "dth_inicio" in df_filtered.columns:
                    from datetime import datetime
                    start_dt = datetime.combine(start_date, datetime.min.time())
                    end_dt = datetime.combine(end_date, datetime.max.time())
                    df_filtered = df_filtered.filter(
                        (col("dth_inicio") >= start_dt) & (col("dth_inicio") <= end_dt)
                    )

                try:
                    has_rows = len(df_filtered.limit(1).collect()) > 0
                except Exception:
                    has_rows = False

                if has_rows:
                    month_dfs.append(df_filtered)

            except Exception:
                continue

        if not month_dfs:
            return self._create_empty_result_df()

        df = month_dfs[0]
        for d in month_dfs[1:]:
            df = df.unionByName(d, allowMissingColumns=True)

        # Filter by dism
        df = df.withColumn("id_str", col("id_processo").cast(StringType()))
        dism_df = dism_df.withColumn("id_str", col("id_processo").cast(StringType()))
        df = df.join(dism_df.select("id_str"), on="id_str", how="inner").drop("id_str")

        return df

    def _calculate_cnm(self) -> DataFrame:
        """
        Calculate CNM (distribution) - new cases in current year.

        For meta_01: Applies historical check to ensure distribution did NOT happen
        before the current year.
        """
        calculation_start = datetime.now()
        self.logger.info(f"🚀 Calculating cnm{self.meta_number} (distribution)")

        distribution_situacoes = self.config_manager.get_situacao_codes("metas", "distribution_situacoes")
        self.logger.info(f"📂 Loading distribution situacoes: {distribution_situacoes}")

        # Load current year distributions
        distr_dfs = []
        for situacao in distribution_situacoes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                try:
                    if not situacao_df.isEmpty():
                        distr_dfs.append(situacao_df)
                except Exception:
                    distr_dfs.append(situacao_df)

        if not distr_dfs:
            self.logger.warning("⚠️ No distribution data found")
            return self._create_empty_result_df()

        result_df = distr_dfs[0]
        for df in distr_dfs[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)

        # Keep first occurrence per process
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        result_df = (result_df
                    .withColumn("row_number", row_number().over(window))
                    .filter(col("row_number") == 1)
                    .drop("row_number"))

        # Historical check for meta_01
        if self.loading_mode == "eligible_processes":
            self.logger.info(f"📂 Loading historical distributions for exclusion...")
            current_year_ids = result_df.select("id_processo").distinct()

            historical_dfs = []
            for situacao in distribution_situacoes:
                hist_df = self._load_situacao_with_eligible_filter(
                    situacao,
                    apply_period_filter=True,
                    period_start=date(1988, 1, 1),
                    period_end=self.meta_cutoff_date - timedelta(days=1)
                )
                if hist_df is not None:
                    # Filter to only current year IDs
                    hist_df = hist_df.join(current_year_ids, "id_processo", "inner")
                    try:
                        if not hist_df.isEmpty():
                            historical_dfs.append(hist_df)
                    except Exception:
                        historical_dfs.append(hist_df)

            if historical_dfs:
                historical_df = historical_dfs[0]
                for df in historical_dfs[1:]:
                    historical_df = historical_df.unionByName(df, allowMissingColumns=True)

                historical_ids = historical_df.select("id_processo").distinct()
                result_df = result_df.join(historical_ids, "id_processo", "left_anti")
                self.logger.info("✅ Applied historical exclusion")

        if result_df is None or result_df.isEmpty():
            return self._create_empty_result_df()

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"cnm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end))
                    .withColumn("ano", year(col("dth_inicio")))
                    .withColumn("mes", month(col("dth_inicio"))))

        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ cnm{self.meta_number} completed in {calculation_time:.2f}s")

        return result_df

    def _calculate_primeirasentm(self) -> DataFrame:
        """Calculate primeirasentm (first sentences)."""
        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating primeirasentm{self.meta_number}...")

        prim_sent_situacoes = self.config_manager.get_situacao_codes("metas", "prim_sent_situacoes")
        self.logger.info(f"📂 Loading sentencing situacoes: {prim_sent_situacoes}")

        sent_dfs = []
        for situacao in prim_sent_situacoes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                try:
                    if not situacao_df.isEmpty():
                        sent_dfs.append(situacao_df)
                except Exception:
                    sent_dfs.append(situacao_df)

        if not sent_dfs:
            self.logger.error("❌ No sentencing data found")
            return self._create_empty_result_df()

        result_df = sent_dfs[0]
        for df in sent_dfs[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)

        # Keep first sentence per process
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        result_df = (result_df
                    .withColumn("rn", row_number().over(window))
                    .filter(col("rn") == 1)
                    .drop("rn"))

        # Historical check for meta_01
        if self.loading_mode == "eligible_processes":
            self.logger.info("📂 Loading historical sentences for exclusion...")
            current_ids = result_df.select("id_processo").distinct()

            historical_dfs = []
            for situacao in prim_sent_situacoes:
                hist_df = self._load_situacao_with_eligible_filter(
                    situacao,
                    apply_period_filter=True,
                    period_start=date(1988, 1, 1),
                    period_end=self.meta_cutoff_date - timedelta(days=1)
                )
                if hist_df is not None:
                    hist_df = hist_df.join(current_ids, "id_processo", "inner")
                    try:
                        if not hist_df.isEmpty():
                            historical_dfs.append(hist_df)
                    except Exception:
                        historical_dfs.append(hist_df)

            if historical_dfs:
                historical_df = historical_dfs[0]
                for df in historical_dfs[1:]:
                    historical_df = historical_df.unionByName(df, allowMissingColumns=True)

                historical_ids = historical_df.select("id_processo").distinct()
                result_df = result_df.join(historical_ids, "id_processo", "left_anti")
                self.logger.info("✅ Applied historical exclusion")

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"primeirasentm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        self.logger.info(f"✅ primeirasentm{self.meta_number} completed")
        return result_df

    def _calculate_baixm(self) -> DataFrame:
        """Calculate baixm (archived/closed processes)."""
        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating baixm{self.meta_number}...")

        baixa_situacoes = self.config_manager.get_situacao_codes("metas", "baixa_situacoes")
        self.logger.info(f"📂 Loading baixa situacoes: {baixa_situacoes}")

        baixa_dfs = []
        for situacao in baixa_situacoes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                try:
                    if not situacao_df.isEmpty():
                        baixa_dfs.append(situacao_df)
                except Exception:
                    baixa_dfs.append(situacao_df)

        if not baixa_dfs:
            self.logger.error("❌ No baixa data found")
            return self._create_empty_result_df()

        result_df = baixa_dfs[0]
        for df in baixa_dfs[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)

        # Keep first occurrence per process
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        result_df = (result_df
                    .withColumn("rn", row_number().over(window))
                    .filter(col("rn") == 1)
                    .drop("rn"))

        # Historical check for meta_01
        if self.loading_mode == "eligible_processes":
            self.logger.info("📂 Loading historical baixas for exclusion...")
            current_ids = result_df.select("id_processo").distinct()

            historical_dfs = []
            for situacao in baixa_situacoes:
                hist_df = self._load_situacao_with_eligible_filter(
                    situacao,
                    apply_period_filter=True,
                    period_start=date(1988, 1, 1),
                    period_end=self.meta_cutoff_date - timedelta(days=1)
                )
                if hist_df is not None:
                    hist_df = hist_df.join(current_ids, "id_processo", "inner")
                    try:
                        if not hist_df.isEmpty():
                            historical_dfs.append(hist_df)
                    except Exception:
                        historical_dfs.append(hist_df)

            if historical_dfs:
                historical_df = historical_dfs[0]
                for df in historical_dfs[1:]:
                    historical_df = historical_df.unionByName(df, allowMissingColumns=True)

                historical_ids = historical_df.select("id_processo").distinct()
                result_df = result_df.join(historical_ids, "id_processo", "left_anti")
                self.logger.info("✅ Applied historical exclusion")

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"baixm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        self.logger.info(f"✅ baixm{self.meta_number} completed")
        return result_df

    def _calculate_mpum(self) -> DataFrame:
        """Calculate mpum (protective measures and decisions)."""
        calculation_start = datetime.now()
        self.logger.info(f"🚀 Calculating mpum{self.meta_number}...")

        situacao_codes = [17, 34]
        valid_movements = self.config_manager.get_situacao_codes("metas", "valid_movement_codes")

        mpu_dfs = []
        for situacao in situacao_codes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                try:
                    if not situacao_df.isEmpty():
                        mpu_dfs.append(situacao_df)
                except Exception:
                    mpu_dfs.append(situacao_df)

        if not mpu_dfs:
            self.logger.error("❌ No mpum data found")
            return self._create_empty_result_df()

        result_df = mpu_dfs[0]
        for df in mpu_dfs[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)

        # Filter by valid movements
        if "cod_mni_movimento" in result_df.columns:
            result_df = result_df.filter(col("cod_mni_movimento").isin(valid_movements))

        # Keep first occurrence per process
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        result_df = (result_df
                    .withColumn("rn", row_number().over(window))
                    .filter(col("rn") == 1)
                    .drop("rn"))

        # Historical check for meta_01
        if self.loading_mode == "eligible_processes":
            self.logger.info("📂 Loading historical mpum for exclusion...")
            current_ids = result_df.select("id_processo").distinct()

            historical_dfs = []
            for situacao in situacao_codes:
                hist_df = self._load_situacao_with_eligible_filter(
                    situacao,
                    apply_period_filter=True,
                    period_start=date(1988, 1, 1),
                    period_end=self.meta_cutoff_date - timedelta(days=1)
                )
                if hist_df is not None:
                    if "cod_mni_movimento" in hist_df.columns:
                        hist_df = hist_df.filter(col("cod_mni_movimento").isin(valid_movements))
                    hist_df = hist_df.join(current_ids, "id_processo", "inner")
                    try:
                        if not hist_df.isEmpty():
                            historical_dfs.append(hist_df)
                    except Exception:
                        historical_dfs.append(hist_df)

            if historical_dfs:
                historical_df = historical_dfs[0]
                for df in historical_dfs[1:]:
                    historical_df = historical_df.unionByName(df, allowMissingColumns=True)

                historical_ids = historical_df.select("id_processo").distinct()
                result_df = result_df.join(historical_ids, "id_processo", "left_anti")
                self.logger.info("✅ Applied historical exclusion")

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"mpum{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        self.logger.info(f"✅ mpum{self.meta_number} completed")
        return result_df

    def _calculate_julgadom(self) -> DataFrame:
        """Calculate julgadom (judged processes) - composite variable."""
        calculation_start = datetime.now()
        self.logger.info(f"🚀 Calculating julgadom{self.meta_number}...")

        # Load constituent variables from parquet
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "eproc2g")
        constituent_results = []

        for variable in [f"primeirasentm{self.meta_number}",
                        f"baixm{self.meta_number}",
                        f"mpum{self.meta_number}"]:
            parquet_path = f"{_meta_export_path}/{variable}.parquet"
            self.logger.info(f"📂 Looking for: {parquet_path}")

            try:
                if os.path.exists(parquet_path):
                    df = self.spark.read.option("recursiveFileLookup", True).parquet(parquet_path)
                    if df is not None:
                        constituent_results.append(df)
                        self.logger.info(f"✅ Loaded {variable}")
            except Exception as e:
                self.logger.error(f"❌ Error reading {variable}: {e}")

        if not constituent_results:
            self.logger.error("❌ No constituent results found for julgadom")
            return self._create_empty_result_df()

        # Union all constituent results
        result_df = constituent_results[0]
        for df in constituent_results[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)

        # Keep first occurrence per process
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        result_df = (result_df
                    .withColumn("rn", row_number().over(window))
                    .filter(col("rn") == 1)
                    .drop("rn"))

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"julgadom{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        self.logger.info(f"✅ julgadom{self.meta_number} completed")
        return result_df

    def _calculate_decm(self) -> DataFrame:
        """Calculate decm (decisions in month)."""
        self.logger.info(f"🚀 Calculating decm{self.meta_number}...")

        decm_codes = self.config_manager.get_situacao_codes("metas", "decm_situacoes")
        self.logger.info(f"📂 Loading decm situacoes: {decm_codes}")

        decm_dfs = []
        for code in decm_codes:
            df = self.load_situacao_unified(
                code,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if df is not None:
                try:
                    if not df.isEmpty():
                        decm_dfs.append(df)
                except Exception:
                    decm_dfs.append(df)

        if not decm_dfs:
            self.logger.warning(f"No decm{self.meta_number} data found")
            return self._create_empty_result_df()

        result_df = decm_dfs[0]
        for df in decm_dfs[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)

        # Keep first occurrence per process
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        result_df = (result_df
                    .withColumn("rn", row_number().over(window))
                    .filter(col("rn") == 1)
                    .drop("rn"))

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"decm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        self.logger.info(f"✅ decm{self.meta_number} completed")
        return result_df

    def _calculate_susm(self) -> DataFrame:
        """Calculate susm (suspended processes)."""
        self.logger.info(f"🎯 Calculating susm{self.meta_number}...")

        # Load julgadom for exclusion
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "eproc2g")
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}.parquet"

        try:
            julgadom_df = self.spark.read.parquet(julgadom_path).select("id_processo").distinct()
        except Exception as e:
            self.logger.warning(f"Could not load julgadom for exclusion: {e}")
            julgadom_df = self.spark.createDataFrame([], StructType([
                StructField("id_processo", StringType(), True)
            ]))

        susp_situacoes = self.config_manager.get_situacao_codes("metas", "suspenso_situacoes")
        self.logger.info(f"📂 Loading suspenso situacoes: {susp_situacoes}")

        susp_dfs = []
        for situacao in susp_situacoes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                try:
                    if not situacao_df.isEmpty():
                        susp_dfs.append(situacao_df)
                except Exception:
                    susp_dfs.append(situacao_df)

        if not susp_dfs:
            self.logger.error("❌ No suspenso data found")
            return self._create_empty_result_df()

        result_df = susp_dfs[0]
        for df in susp_dfs[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)

        # Keep first occurrence per process
        window = Window.partitionBy("id_processo").orderBy(asc("dth_inicio"))
        result_df = (result_df
                    .withColumn("_rn", row_number().over(window))
                    .filter(col("_rn") == 1)
                    .drop("_rn"))

        # Remove julgados
        result_df = result_df.join(julgadom_df, "id_processo", "left_anti")

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"susm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        self.logger.info(f"✅ susm{self.meta_number} completed")
        return result_df

    def _calculate_desm(self) -> DataFrame:
        """Calculate desm (unsuspended processes)."""
        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating desm{self.meta_number}...")

        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "eproc2g")

        # Load situacao 25 (suspension)
        situacao_25 = self.load_situacao_unified(
            situacao=25,
            apply_period_filter=True,
            period_start=self.meta_cutoff_date,
            period_end=self.period_end
        )

        if situacao_25 is None or situacao_25.isEmpty():
            self.logger.warning("No situacao 25 data found")
            return self._create_empty_result_df()

        # Keep earliest opened 25 per process (first unsuspension)
        window = Window.partitionBy("id_processo").orderBy(col("dth_inicio").asc())
        first_25 = (situacao_25
                   .withColumn("row_num", row_number().over(window))
                   .filter(col("row_num") == 1)
                   .drop("row_num"))
        first_opened_25 = first_25.filter(col("dth_fim").isNull())

        if first_opened_25 is None or first_opened_25.isEmpty():
            self.logger.warning("No opened situacao 25 found")
            return self._create_empty_result_df()

        # Remove julgados
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}.parquet"
        try:
            julgadom_df = self.spark.read.parquet(julgadom_path).select("id_processo")
            result_df = first_opened_25.join(julgadom_df, "id_processo", "left_anti")
            self.logger.info("✅ Removed julgados successfully")
        except Exception as e:
            self.logger.warning(f"Could not load julgadom for exclusion: {e}")
            result_df = first_opened_25

        # Add metadata
        result_df = (result_df
                    .withColumn("variavel", lit(f"desm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ desm{self.meta_number} completed in {calculation_time:.2f}s")

        return result_df
