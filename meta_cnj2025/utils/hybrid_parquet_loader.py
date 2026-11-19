import os
import glob
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime, date

# PySpark imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, lit, trim, substring_index, when
    from pyspark.sql.types import StringType, LongType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


class HybridParquetLoader:
    
    def __init__(self, spark_session: SparkSession, logger, misc, apply_meta03_filter: bool = True, 
                 minimal_logging: bool = True, incremental_months: int = 0):
        """Initialize with existing Spark session."""
        self.spark = spark_session
        self.logger = logger
        self.misc = misc
        self.apply_meta03_filter = apply_meta03_filter
        self.incremental_months = incremental_months
        self.minimal_logging = minimal_logging
        
        if self.minimal_logging:
            try:
                self.logger.setLevel(logging.WARNING)
            except Exception:
                pass

    def get_recently_updated_processes(self, incremental_months: int = 2, situacao_base_path: str = None) -> DataFrame:
        """
        Scan recent partitions to identify processes with updates.
        
        Args:
            incremental_months: Number of recent months to scan (default: 2)
            situacao_base_path: Base path for situacao data
            
        Returns:
            DataFrame with 'coddoc' column containing recently updated processes
        """
        from dateutil.relativedelta import relativedelta
        
        if situacao_base_path is None:
            from .parquet_utils import ejud_situacao_base
            situacao_base_path = ejud_situacao_base()
        
        # Calculate recent months
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=incremental_months - 1)
        
        months = []
        current = start_date
        while current <= end_date:
            months.append(current.strftime("%Y-%m"))
            current = current + relativedelta(months=1)
        
        # Build file patterns for recent months
        file_patterns = [f"{situacao_base_path}/year_month={m}/*.parquet" for m in months]
        
        self.logger.info(f"🔍 Scanning {incremental_months} recent months: {months}")
        
        # Expand patterns
        parquet_files = []
        for pat in file_patterns:
            parquet_files.extend(glob.glob(pat))
        
        if not parquet_files:
            self.logger.warning(f"⚠️ No parquet files found in recent {incremental_months} months")
            return self.spark.createDataFrame([], schema="coddoc long")
        
        # Read and get distinct coddocs
        df = self.spark.read.parquet(*parquet_files)
        updated_coddocs = df.select("coddoc").distinct()
        
        self.logger.info(f"✅ Found updated processes in last {incremental_months} months")
        
        return updated_coddocs

    def load_situacao_with_coddoc_filter(self, situacao: int, 
                                        coddoc_list,
                                        situacao_base_path: str = None,
                                        date_column: str = "dthrmov",
                                        apply_period_filter: bool = False,
                                        period_start: Optional[date] = None,
                                        period_end: Optional[date] = None,
                                        preserve_eligible_columns: bool = False) -> DataFrame:
        """
        Load situacao parquet files filtered by coddoc list.
        Simple join approach - let Spark optimize.
        """
        try:
            self.logger.info(f"🔄 Loading situacao {situacao}")
            
            # Resolve default path
            if situacao_base_path is None:
                from .parquet_utils import ejud_situacao_base
                situacao_base_path = ejud_situacao_base()
            
            # Build file patterns based on date filter
            file_patterns = []
            if apply_period_filter and (period_start is not None or period_end is not None):
                # Generate month partitions covering date range
                months = self._generate_month_range(period_start, period_end)
                for ym in months:
                    file_patterns.append(f"{situacao_base_path}/year_month={ym}/*.parquet")
            else:
                # Load all months
                file_patterns.append(f"{situacao_base_path}/year_month=*/*.parquet")
            
            # Expand file patterns
            parquet_files = []
            for pat in file_patterns:
                parquet_files.extend(glob.glob(pat))
            
            if not parquet_files:
                self.logger.error(f"❌ No parquet files found for patterns: {file_patterns}")
                return self.misc._create_empty_result_df()
            
            self.logger.info(f"📂 Reading {len(parquet_files)} parquet files")

            # Read parquet files using schema inferred from the first file.
            # All partitions were rewritten with a canonical schema (coddoc: STRING)
            # by rewrite_situacao_schema.py, so a single consistent read works.
            df = self.spark.read.parquet(*parquet_files)

            # Filter by situacao (handle both padded and unpadded formats)
            if "situacao" not in df.columns:
                self.logger.error(f"❌ No 'situacao' column found!")
                return self.misc._create_empty_result_df()

            situacao_padded = str(situacao).zfill(3)
            situacao_unpadded = str(situacao)

            df = df.filter(
                (col("situacao").cast("string") == situacao_padded) |
                (col("situacao").cast("string") == situacao_unpadded)
            )

            # Create normalized coddoc_norm column for joins
            df = self._add_coddoc_norm(df)
            
            # Prepare coddoc filter DataFrame
            coddoc_df = self._prepare_coddoc_filter(coddoc_list, 'coddoc_norm')
            
            # Apply coddoc filter - simple join
            if coddoc_df is not None:
                self.logger.info(f"📊 Applying coddoc filter join")
                df = df.join(coddoc_df, on='coddoc_norm', how="inner")
            
            # Apply date filtering
            if apply_period_filter and (period_start or period_end):
                self.logger.info(f"🗓️ Applying date filter on '{date_column}'")
                
                if period_start:
                    df = df.filter(col(date_column) >= lit(period_start))
                
                if period_end:
                    df = df.filter(col(date_column) <= lit(period_end))
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error loading situacao {situacao}: {e}")
            return self.misc._create_empty_result_df()

    def load_situacao_with_id_processo_filter(self, situacao: int, 
                                        id_processo_list,
                                        situacao_base_path: str = None,
                                        date_column: str = "dth_inicio",
                                        apply_period_filter: bool = False,
                                        period_start: Optional[date] = None,
                                        period_end: Optional[date] = None,
                                        preserve_eligible_columns: bool = False) -> DataFrame:
        """
        Load situacao parquet files filtered by id_processo list.
        Simple join approach - let Spark optimize.
        """
        try:
            self.logger.info(f"🔄 Loading situacao {situacao} with id_processo filter")
            
            # Use default path if not provided
            if situacao_base_path is None:
                from .parquet_utils import eproc_situacao_base
                situacao_base_path = eproc_situacao_base()
            
            # Build file patterns based on date filter
            file_patterns = []
            if apply_period_filter and (period_start is not None or period_end is not None):
                months = self._generate_month_range(period_start, period_end)
                for ym in months:
                    file_patterns.append(f"{situacao_base_path}/year_month={ym}/*.parquet")
            else:
                file_patterns.append(f"{situacao_base_path}/year_month=*/*.parquet")
            
            # Expand patterns
            parquet_files = []
            for pat in file_patterns:
                parquet_files.extend(glob.glob(pat))
            
            # Try alternative base if no files found
            if not parquet_files:
                parquet_files = self._try_alternative_base(situacao_base_path, file_patterns)
            
            if not parquet_files:
                self.logger.error(f"❌ No parquet files found!")
                return self.misc._create_empty_result_df()
            
            self.logger.info(f"📂 Reading {len(parquet_files)} parquet files")
            
            # Read parquet files
            df = self.spark.read.parquet(*parquet_files)
            
            # Filter by situacao (handle both padded and unpadded formats)
            if "situacao" not in df.columns:
                self.logger.error(f"❌ No 'situacao' column found!")
                return self.misc._create_empty_result_df()
            
            situacao_padded = str(situacao).zfill(3)
            situacao_unpadded = str(situacao)
            
            df = df.filter(
                (col("situacao").cast("string") == situacao_padded) |
                (col("situacao").cast("string") == situacao_unpadded)
            )
            
            # Prepare id_processo filter DataFrame
            id_processo_df = self._prepare_id_processo_filter(id_processo_list, preserve_eligible_columns)
            
            # Apply id_processo filter - simple join
            if id_processo_df is not None:
                self.logger.info(f"📊 Applying id_processo filter join")
                df = df.join(id_processo_df, on="id_processo", how="inner")
            
            # Apply date filtering
            if apply_period_filter and (period_start is not None or period_end is not None):
                self.logger.info(f"🗓️ Applying date filter on '{date_column}'")
                
                if period_start:
                    period_start_dt = datetime.combine(period_start, datetime.min.time()) if isinstance(period_start, date) else period_start
                    df = df.filter(col(date_column) >= period_start_dt)
                
                if period_end:
                    period_end_dt = datetime.combine(period_end, datetime.min.time()) if isinstance(period_end, date) else period_end
                    df = df.filter(col(date_column) <= period_end_dt)
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error loading situacao {situacao}: {e}")
            self.logger.exception("Full error traceback:")
            return self.misc._create_empty_result_df()

    # Helper methods
    
    def _generate_month_range(self, start_date: Optional[date], end_date: Optional[date]) -> list:
        """Generate list of YYYY-MM strings for date range."""
        if start_date is None:
            raise ValueError("start_date is required when using period filter. Check your config.json period_start setting.")
        if end_date is None:
            raise ValueError("end_date is required when using period filter. Check your config.json period_end setting.")
        
        # Convert datetime to date if needed
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()
        
        months = []
        current = date(start_date.year, start_date.month, 1)
        
        while current <= end_date:
            months.append(current.strftime("%Y-%m"))
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
        
        return months
    
    def _add_coddoc_norm(self, df: DataFrame) -> DataFrame:
        """Add normalized coddoc_norm column for joins."""
        # Keep original coddoc as string
        if "coddoc" in df.columns:
            df = df.withColumn("coddoc_str", col("coddoc").cast("string"))
        
        # Create or normalize coddoc_norm
        if 'coddoc_norm' in df.columns:
            df = df.withColumn('coddoc_norm', col('coddoc_norm').cast('long'))
        elif "coddoc" in df.columns:
            df = df.withColumn('coddoc_norm', trim(substring_index(col('coddoc').cast('string'), '.', 1)))
            df = df.withColumn('coddoc_norm', when(col('coddoc_norm') == '', None).otherwise(col('coddoc_norm').cast('long')))
        else:
            df = df.withColumn('coddoc_norm', lit(None).cast('long'))
        
        return df
    
    def _prepare_coddoc_filter(self, coddoc_list, join_col: str = 'coddoc_norm') -> Optional[DataFrame]:
        """Prepare coddoc filter as DataFrame."""
        if coddoc_list is None:
            return None
        
        # If already a DataFrame
        if hasattr(coddoc_list, 'select') and hasattr(coddoc_list, 'columns'):
            src_col = 'coddoc'
            coddoc_df = coddoc_list.select(src_col).distinct()
            # Normalize to join_col format
            coddoc_df = coddoc_df.withColumn(f"{join_col}_str", trim(substring_index(col(src_col).cast("string"), '.', 1)))
            coddoc_df = coddoc_df.withColumn(join_col, when(col(f"{join_col}_str") == '', None).otherwise(col(f"{join_col}_str").cast("long")))
            return coddoc_df.select(join_col).distinct()
        
        # If Python list/tuple/set
        if isinstance(coddoc_list, (list, tuple, set)):
            tmp_rows = [(v,) for v in coddoc_list]
            tmp_df = self.spark.createDataFrame(tmp_rows, ['coddoc'])
            tmp_df = tmp_df.withColumn('coddoc_raw_str', trim(substring_index(col('coddoc').cast('string'), '.', 1)))
            tmp_df = tmp_df.withColumn(join_col, when(col('coddoc_raw_str') == '', None).otherwise(col('coddoc_raw_str').cast('long')))
            return tmp_df.select(join_col).distinct()
        
        # Single value
        tmp_df = self.spark.createDataFrame([(coddoc_list,)], ['coddoc'])
        tmp_df = tmp_df.withColumn('coddoc_raw_str', trim(substring_index(col('coddoc').cast('string'), '.', 1)))
        tmp_df = tmp_df.withColumn(join_col, when(col('coddoc_raw_str') == '', None).otherwise(col('coddoc_raw_str').cast('long')))
        return tmp_df.select(join_col).distinct()
    
    def _prepare_id_processo_filter(self, id_processo_list, preserve_columns: bool = False) -> Optional[DataFrame]:
        """Prepare id_processo filter as DataFrame."""
        if id_processo_list is None:
            return None
        
        # If already a DataFrame
        if hasattr(id_processo_list, 'select') and hasattr(id_processo_list, 'columns'):
            if preserve_columns and len(id_processo_list.columns) > 1:
                # Keep all columns for join
                return id_processo_list
            else:
                # Just id_processo column
                return id_processo_list.select("id_processo").distinct()
        
        # If Python list/tuple/set
        if isinstance(id_processo_list, (list, tuple, set)):
            return self.spark.createDataFrame([(v,) for v in id_processo_list], ["id_processo"])
        
        # Single value
        return self.spark.createDataFrame([(id_processo_list,)], ["id_processo"])
    
    def _try_alternative_base(self, current_base: str, file_patterns: list) -> list:
        """Try alternative base path (eproc vs ejud) if files not found."""
        try:
            from .parquet_utils import ejud_situacao_base, eproc_situacao_base
            
            default_ejud = ejud_situacao_base()
            alt_base = None
            
            if current_base and str(current_base).startswith(str(default_ejud)):
                alt_base = eproc_situacao_base()
            else:
                alt_base = ejud_situacao_base()
            
            if alt_base and alt_base != current_base:
                self.logger.info(f"ℹ️ Trying alternative base: {alt_base}")
                alt_parquet_files = []
                for pat in file_patterns:
                    alt_pat = pat.replace(str(current_base), str(alt_base))
                    alt_parquet_files.extend(glob.glob(alt_pat))
                
                if alt_parquet_files:
                    self.logger.info(f"📂 Found {len(alt_parquet_files)} files under alternative base")
                    return alt_parquet_files
        except Exception as e:
            self.logger.warning(f"⚠️ Could not try alternative base: {e}")
        
        return []
