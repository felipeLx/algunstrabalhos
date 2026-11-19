"""
EPROC Meta Stocks Calculator

This module provides stock calculation functionality for EPROC data,
similar to meta_stocks.py for EJUD.

Key differences from EJUD:
- Uses id_processo (string) instead of coddoc (long)
- Uses dth_inicio/dth_fim instead of dthrmov/dth_fim
- Uses year_month partitioned situacao data
- Uses different table structures from bronze layer

Stock Variables:
- dism: Distributed but not judged processes
- dism_ant: Historical distributed but not judged (15+ years old)
- pend_old_88: Pending status (situacao 88) historical
- pend_old_25: Suspended status (situacao 25) historical
"""

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import os
import logging
import gc

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, count, sum as spark_sum,
        max as spark_max, min as spark_min, first, desc, asc,
        year, month, dayofmonth, to_date, date_format,
        broadcast, coalesce, current_timestamp, concat_ws,
        row_number, rank, dense_rank
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    from pyspark import StorageLevel
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from utils.parquet_utils import meta_export_path, eproc_situacao_base


class MetaCalculatorEproc:
    """
    EPROC Meta Stock Calculator.

    Provides methods for calculating stock variables (dism, dism_ant, etc.)
    for EPROC data sources.
    """

    def __init__(self, spark, config_manager, logger, misc, eligible_processes_df, meta_logger,
                 period_start, period_end, meta_cutoff_date, meta_15year_cutoff, meta_number,
                 situacao_base_path: str = None):
        """
        Initialize EPROC Meta Calculator.

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

        # Set situacao base path
        self.situacao_base_path = situacao_base_path or eproc_situacao_base()

        # Empty schema for fallback
        self.empty_schema = StructType([
            StructField("id_processo", StringType(), True),
            StructField("dth_inicio", TimestampType(), True),
            StructField("dth_fim", TimestampType(), True),
            StructField("situacao", IntegerType(), True)
        ])

    def _create_empty_result_df(self) -> DataFrame:
        """Create an empty DataFrame with standard result schema."""
        return self.spark.createDataFrame([], self.empty_schema)

    def _load_situacao_by_month(self, situacao: int,
                                 period_start: date,
                                 period_end: date,
                                 eligible_processes: DataFrame = None) -> DataFrame:
        """
        Load situacao data from year_month partitioned structure.

        Args:
            situacao: Situacao code to load
            period_start: Start date for filtering
            period_end: End date for filtering
            eligible_processes: Optional DataFrame with eligible id_processo for filtering

        Returns:
            Filtered DataFrame with situacao data
        """
        from datetime import datetime
        import glob as glob_module

        self.logger.info(f"📂 Loading situacao {situacao} from {period_start} to {period_end}")

        # Build month range to scan
        try:
            cur = period_start.replace(day=1)
            months = []
            while cur <= period_end:
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
                    self.logger.debug(f"ℹ️ 'situacao' column missing in {ym_dir}")
                    continue

                # Apply date filter
                if "dth_inicio" in df_filtered.columns:
                    start_dt = datetime.combine(period_start, datetime.min.time())
                    end_dt = datetime.combine(period_end, datetime.max.time())
                    df_filtered = df_filtered.filter(
                        (col("dth_inicio") >= start_dt) & (col("dth_inicio") <= end_dt)
                    )

                # Check if we have any data
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

        # Filter by eligible processes if provided
        if eligible_processes is not None:
            eligible_str = eligible_processes.select("id_processo").distinct()
            df = df.withColumn("id_str", col("id_processo").cast(StringType()))
            eligible_str = eligible_str.withColumn("id_str", col("id_processo").cast(StringType()))
            df = df.join(eligible_str.select("id_str"), on="id_str", how="inner").drop("id_str")

        return df

    def _calculate_dism_eproc(self) -> DataFrame:
        """
        Calculate DISM for EPROC: Distributed but not judged processes.

        Logic:
        1. Load distribution situacoes within the period
        2. Load judgment situacoes within the period
        3. Remove judged processes from distributed
        4. Keep first occurrence per process
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number
        from pyspark.sql.window import Window

        self.logger.info("🚀 Calculating EPROC dism (distributed but not judged)...")

        try:
            if self.eligible_processes_df is None:
                self.logger.error("❌ No eligible processes available")
                return self._create_empty_result_df()

            cutoff_date = self.meta_cutoff_date or self.period_end
            distribution_start = date(1980, 1, 1)

            # Load distribution situacoes
            distribution_situacoes = self.config_manager.get_situacao_codes("metas", "distribution_situacoes")
            self.logger.info(f"📂 Loading distribution situacoes: {distribution_situacoes}")

            distr_frames = []
            for situacao in distribution_situacoes:
                situacao_df = self._load_situacao_by_month(
                    situacao=situacao,
                    period_start=distribution_start,
                    period_end=cutoff_date,
                    eligible_processes=self.eligible_processes_df
                )
                if situacao_df is not None:
                    try:
                        if not situacao_df.isEmpty():
                            distr_frames.append(situacao_df)
                    except Exception:
                        distr_frames.append(situacao_df)

            if not distr_frames:
                self.logger.warning("⚠️ No distribution data found")
                return self._create_empty_result_df()

            distribuicao_df = distr_frames[0]
            for df in distr_frames[1:]:
                distribuicao_df = distribuicao_df.unionByName(df, allowMissingColumns=True)

            # Load judgment situacoes
            judgment_situacoes = self.config_manager.get_situacao_codes("metas", "judgment_situacoes")
            self.logger.info(f"📂 Loading judgment situacoes: {judgment_situacoes}")

            julgamento_frames = []
            for situacao in judgment_situacoes:
                situacao_df = self._load_situacao_by_month(
                    situacao=situacao,
                    period_start=distribution_start,
                    period_end=cutoff_date,
                    eligible_processes=distribuicao_df
                )
                if situacao_df is not None:
                    try:
                        if not situacao_df.isEmpty():
                            julgamento_frames.append(situacao_df)
                    except Exception:
                        julgamento_frames.append(situacao_df)

            if julgamento_frames:
                julgamento_df = julgamento_frames[0]
                for df in julgamento_frames[1:]:
                    julgamento_df = julgamento_df.unionByName(df, allowMissingColumns=True)

                # Remove judged processes
                result_df = distribuicao_df.join(
                    julgamento_df.select("id_processo").distinct(),
                    on="id_processo",
                    how="left_anti"
                )
            else:
                result_df = distribuicao_df

            # Keep first occurrence per process
            window = Window.partitionBy("id_processo").orderBy(col("dth_inicio"))
            result_df = (result_df
                        .withColumn("rn", row_number().over(window))
                        .filter(col("rn") == 1)
                        .drop("rn"))

            # Add metadata
            result_df = (result_df
                        .withColumn("variavel", lit(f"dism{self.meta_number}"))
                        .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                        .withColumn("dth_referencia", lit(self.period_end))
                        .withColumn("dth_calculo", current_timestamp())
                        .withColumn("periodo_inicio", lit(self.period_start))
                        .withColumn("periodo_fim", lit(self.period_end)))

            self.logger.info("✅ EPROC dism calculation completed")
            return result_df

        except Exception as e:
            self.logger.error(f"❌ Error in EPROC dism calculation: {e}")
            import traceback
            self.logger.error(f"📋 Traceback: {traceback.format_exc()}")
            return self._create_empty_result_df()

    def _calculate_dism_ant_eproc(self) -> DataFrame:
        """
        Calculate DISM_ANT for EPROC: Historical distributed but not judged (15+ years old).
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number
        from pyspark.sql.window import Window

        self.logger.info("🚀 Calculating EPROC dism_ant (historical distributed but not judged)...")

        try:
            if self.eligible_processes_df is None:
                self.logger.error("❌ No eligible processes available")
                return self._create_empty_result_df()

            period_start = date(1980, 1, 1)
            period_end = self.meta_15year_cutoff

            # Load distribution situacoes
            distribution_situacoes = self.config_manager.get_situacao_codes("metas", "distribution_situacoes")
            self.logger.info(f"📂 Loading distribution situacoes: {distribution_situacoes}")

            distr_frames = []
            for situacao in distribution_situacoes:
                situacao_df = self._load_situacao_by_month(
                    situacao=situacao,
                    period_start=period_start,
                    period_end=period_end,
                    eligible_processes=self.eligible_processes_df
                )
                if situacao_df is not None:
                    try:
                        if not situacao_df.isEmpty():
                            distr_frames.append(situacao_df)
                    except Exception:
                        distr_frames.append(situacao_df)

            if not distr_frames:
                self.logger.warning("⚠️ No historical distribution data found")
                return self._create_empty_result_df()

            distribuicao_df = distr_frames[0]
            for df in distr_frames[1:]:
                distribuicao_df = distribuicao_df.unionByName(df, allowMissingColumns=True)

            # Load judgment situacoes (up to end of previous year)
            judgment_situacoes = self.config_manager.get_situacao_codes("metas", "judgment_situacoes")
            judgment_end = date(self.period_end.year - 1, 12, 31)

            julgamento_frames = []
            for situacao in judgment_situacoes:
                situacao_df = self._load_situacao_by_month(
                    situacao=situacao,
                    period_start=period_start,
                    period_end=judgment_end,
                    eligible_processes=distribuicao_df
                )
                if situacao_df is not None:
                    try:
                        if not situacao_df.isEmpty():
                            julgamento_frames.append(situacao_df)
                    except Exception:
                        julgamento_frames.append(situacao_df)

            if julgamento_frames:
                julgamento_df = julgamento_frames[0]
                for df in julgamento_frames[1:]:
                    julgamento_df = julgamento_df.unionByName(df, allowMissingColumns=True)

                # Remove judged processes
                result_df = distribuicao_df.join(
                    julgamento_df.select("id_processo").distinct(),
                    on="id_processo",
                    how="left_anti"
                )
            else:
                result_df = distribuicao_df

            # Keep first occurrence per process
            window = Window.partitionBy("id_processo").orderBy(col("dth_inicio"))
            result_df = (result_df
                        .withColumn("rn", row_number().over(window))
                        .filter(col("rn") == 1)
                        .drop("rn"))

            # Add metadata
            result_df = (result_df
                        .withColumn("variavel", lit(f"dism{self.meta_number}_ant"))
                        .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                        .withColumn("dth_referencia", lit(self.period_end))
                        .withColumn("dth_calculo", current_timestamp())
                        .withColumn("periodo_inicio", lit(period_start))
                        .withColumn("periodo_fim", lit(period_end)))

            self.logger.info("✅ EPROC dism_ant calculation completed")
            return result_df

        except Exception as e:
            self.logger.error(f"❌ Error in EPROC dism_ant calculation: {e}")
            import traceback
            self.logger.error(f"📋 Traceback: {traceback.format_exc()}")
            return self._create_empty_result_df()

    def _calculate_pend_88_eproc(self) -> DataFrame:
        """
        Calculate pending status (situacao 88) historical for EPROC.
        Finds processes with open situacao 88 at the start of the period.
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number, desc
        from pyspark.sql.window import Window

        self.logger.info("🚀 Calculating EPROC pend_88 (pending status historical)...")

        try:
            # Load situacao 88 up to period start
            sit_88_df = self._load_situacao_by_month(
                situacao=88,
                period_start=date(1970, 1, 1),
                period_end=self.period_start,
                eligible_processes=self.eligible_processes_df
            )

            if sit_88_df is None or sit_88_df.isEmpty():
                self.logger.warning("⚠️ No situacao 88 data found")
                return self._create_empty_result_df()

            # Get latest 88 per process that is still open (dth_fim is null)
            window = Window.partitionBy("id_processo").orderBy(desc("dth_inicio"))
            result_df = (sit_88_df
                        .withColumn("_rn", row_number().over(window))
                        .filter((col("_rn") == 1) & (col("dth_fim").isNull()))
                        .drop("_rn"))

            # Add metadata
            result_df = (result_df
                        .withColumn("variavel", lit("pend_88_eproc"))
                        .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                        .withColumn("dth_referencia", lit(self.period_end))
                        .withColumn("dth_calculo", current_timestamp())
                        .withColumn("periodo_inicio", lit(self.period_start))
                        .withColumn("periodo_fim", lit(self.period_end)))

            self.logger.info("✅ EPROC pend_88 calculation completed")
            return result_df

        except Exception as e:
            self.logger.error(f"❌ Error in EPROC pend_88 calculation: {e}")
            return self._create_empty_result_df()

    def _calculate_pend_25_eproc(self) -> DataFrame:
        """
        Calculate suspended status (situacao 25) historical for EPROC.
        Finds processes with closed situacao 25 at the start of the period.
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number, desc
        from pyspark.sql.window import Window

        self.logger.info("🚀 Calculating EPROC pend_25 (suspended status historical)...")

        try:
            # Load situacao 25 up to period start
            sit_25_df = self._load_situacao_by_month(
                situacao=25,
                period_start=date(1970, 1, 1),
                period_end=self.period_start,
                eligible_processes=self.eligible_processes_df
            )

            if sit_25_df is None or sit_25_df.isEmpty():
                self.logger.warning("⚠️ No situacao 25 data found")
                return self._create_empty_result_df()

            # Get latest 25 per process that is closed (dth_fim is not null)
            window = Window.partitionBy("id_processo").orderBy(desc("dth_inicio"))
            result_df = (sit_25_df
                        .withColumn("_rn", row_number().over(window))
                        .filter((col("_rn") == 1) & (col("dth_fim").isNotNull()))
                        .drop("_rn"))

            # Add metadata
            result_df = (result_df
                        .withColumn("variavel", lit("pend_25_eproc"))
                        .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                        .withColumn("dth_referencia", lit(self.period_end))
                        .withColumn("dth_calculo", current_timestamp())
                        .withColumn("periodo_inicio", lit(self.period_start))
                        .withColumn("periodo_fim", lit(self.period_end)))

            self.logger.info("✅ EPROC pend_25 calculation completed")
            return result_df

        except Exception as e:
            self.logger.error(f"❌ Error in EPROC pend_25 calculation: {e}")
            return self._create_empty_result_df()

    def calculate_all_stocks(self) -> Dict[str, DataFrame]:
        """
        Calculate all stock variables for EPROC.

        Returns:
            Dictionary mapping variable names to DataFrames
        """
        self.logger.info("🚀 Calculating all EPROC stock variables...")

        results = {}

        stock_calculations = [
            ("dism", self._calculate_dism_eproc),
            ("dism_ant", self._calculate_dism_ant_eproc),
            ("pend_88", self._calculate_pend_88_eproc),
            ("pend_25", self._calculate_pend_25_eproc),
        ]

        for name, calc_method in stock_calculations:
            try:
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"🎯 Calculating {name}")
                self.logger.info(f"{'='*50}")

                result = calc_method()
                if result is not None:
                    results[name] = result
                else:
                    self.logger.warning(f"⚠️ {name} returned None")

            except Exception as e:
                self.logger.error(f"❌ Error calculating {name}: {e}")
                continue

        self.logger.info(f"\n✅ Calculated {len(results)} stock variables")
        return results
