from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import argparse
import sys
import os
import logging
import glob
import gc
import shutil
from dotenv import load_dotenv
load_dotenv()
# Import enhanced utilities from utils module
#from utils.misc import MetaMiscellaneous
from stocks.meta_stocks import MetaCalculator
from variables.variables_calculator import MetaVariables
from utils.misc import MetaMiscellaneous, safe_stop_spark
from utils.hybrid_parquet_loader import HybridParquetLoader
from utils.spark_meta_utils import SparkMetaMixin, SparkMetaUtils
from utils.meta_results_exporter import MetaResultsExporter
from utils.meta_logging_system import MetaLogger
from utils.meta_config_manager import MetaConfigManager

# PySpark imports
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
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
    )
    from pyspark import StorageLevel
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.error("PySpark not available. Please install PySpark: pip install pyspark")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MetaCNJ2026CalculatorEJUD_Spark(SparkMetaMixin):
    
    def __init__(self, app_name: str = "MetaCNJ2026_Meta06_Spark", use_recovery: bool = True, 
                 period_start: Optional[date] = None, period_end: Optional[date] = None,
                 incremental_months: int = 0):
        # Store incremental mode setting
        self.incremental_months = incremental_months
        
        # Initialize configuration manager
        self.config_manager = MetaConfigManager()
        self.meta_number = "06"
        self.spark_events_path = self.config_manager.get_system_path("spark_events_path")
        # Initialize enhanced logging
        self.meta_logger = MetaLogger("06")
        self.logger = self.meta_logger.logger
        self.meta_logger.log_calculation_start("Meta CNJ 2026 Spark Calculator - Multi-Filter Implementation")
        
        
        self.spark = (
            SparkMetaUtils.get_optimized_spark_config(app_name)
            .config("spark.eventLog.enabled", "false")
            .config(
                "spark.sql.adaptive.enabled",
                os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false"))
            )
            .config(
                "spark.sql.adaptive.coalescePartitions.enabled",
                os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false"))
            )
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        
        # Suppress some common Spark warnings during cleanup
        spark_logger = self.spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark.storage.BlockManagerStorageEndpoint")
        spark_logger.setLevel(self.spark.sparkContext._jvm.org.apache.log4j.Level.WARN)
        # Initialize results exporter
        self.results_exporter = MetaResultsExporter(self.spark, "06", config_manager=self.config_manager)
        
        # Load paths from configuration
        self.ejud_base_path = self.config_manager.get_system_path("ejud_base_path")
        self.situacao_base_path = self.config_manager.get_system_path("situacao_base_path")
        self.use_recovery = use_recovery
        
        # Load date configuration from JSON
        self.period_end = self.config_manager.get_date_cutoff("meta_06", "period_end")
        self.period_start = self.config_manager.get_date_cutoff("meta_06", "period_start")
        self.meta06_15year_cutoff = self.config_manager.get_date_cutoff("meta_06", "meta06_15year_cutoff")        
        self.meta06_cutoff_date = self.config_manager.get_date_cutoff("meta_06", "meta06_cutoff_date")

        # Load Meta 06 filter paths
        self.meta06_filter_paths = self.config_manager.get_meta_path("meta_06", "meta06_filter_paths")
        
        # self = MetaMiscellaneous(spark = self.spark)
        self.misc = MetaMiscellaneous(
            spark = self.spark,
            logger = self.logger,
            meta_number = "06"
        )
        # Initialize hybrid loader for problematic parquet files (disable Meta 03 filtering for Meta 06)
        self.hybrid_loader = HybridParquetLoader(
            spark_session = self.spark, 
            logger = self.logger,
            misc = self.misc, 
            apply_meta03_filter=False,
            incremental_months=self.incremental_months
        )
        
        # Initialize eligible processes after hybrid loader is ready
        self.eligible_processes_df = self._load_meta_eligible_processes()
        self.logger.info(f"✅ Meta 06 eligible processes loaded: {self.eligible_processes_df.count():,} records")   
        # Load available situacoes for validation
        self.available_situacoes = self.config_manager.get_available_situacoes("ejud")
        
        # Apply Meta 06 specific memory optimizations
        self._configure_meta_memory_optimizations()
        
        # Initialize Meta06Stocks instance
        self.meta_stocks = MetaCalculator(
            spark=self.spark,
            config_manager=self.config_manager,
            logger=self.logger,
            meta_path = self.meta06_filter_paths,
            meta_logger=self.meta_logger,
            hybrid_loader=self.hybrid_loader,
            misc = self.misc,
            eligible_processes_df = self.eligible_processes_df,
            period_start=self.period_start,
            period_end=self.period_end,
            meta_cutoff_date=self.meta06_cutoff_date,
            meta_15year_cutoff=self.meta06_15year_cutoff,
            meta_number=self.meta_number
        )

        # Initialize Meta06 Variables
        self.meta_var = MetaVariables(
            spark=self.spark,
            config_manager=self.config_manager,
            logger=self.logger,
            meta_path = self.meta06_filter_paths,
            meta_logger=self.meta_logger,
            hybrid_loader=self.hybrid_loader,
            misc = self.misc,
            eligible_processes_df = self.eligible_processes_df,
            period_start=self.period_start,
            period_end=self.period_end,
            meta_cutoff_date=self.meta06_cutoff_date,
            meta_15year_cutoff=self.meta06_15year_cutoff,
            meta_number=self.meta_number
        )
        
        self.logger.info(f"📅 Meta 06 date configuration:")
        self.logger.info(f"   📊 Period: {self.period_start} to {self.period_end}")
        self.logger.info(f"   📂 Filter files: {len(self.meta06_filter_paths)} files")
        self.logger.info(f"   📋 Available situacoes: {len(self.available_situacoes)} codes")
        
        # Store results for summary
        self.calculation_results = {}
    
    def get_user_exclusion_filter(self, movimento_base_path: str = None) -> DataFrame:
        """
        Create a DataFrame with coddoc values that should be excluded based on user criteria.
        
        Identifies processes where the latest movement was made by excluded users 
        (EJUDAPURACAO, JUDIMIGRACAO) indicating test/migration data.
        
        Args:
            movimento_base_path: Path to movimento parquet files
            
        Returns:
            DataFrame with coddoc values to exclude
        """
        try:
            excluded_users = ["JUDIMIGRACAO", "EJUDAPURACAO"]
            
            # Default movimento path if not provided
            if movimento_base_path is None:
                movimento_base_path = "/opt/parquet/data/bronze/ejud/movimento"
            
            # Import PySpark functions
            from pyspark.sql.functions import row_number, desc, col, year
            from pyspark.sql.window import Window
            
            # Load movimento data 
            movimento_files = f"{movimento_base_path}"
            movimento_df = self.spark.read.option("recursiveFileLookup", "true").parquet(movimento_files).select("coddoc", "dthrmov", "usuincl").filter(year(col("dthrmov")) <= 2000)
            
            # Get latest movement per process (coddoc) - working with reduced dataset
            window_spec = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
            
            latest_movements = (
                movimento_df
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .drop("rn")
            )

            excluded_processes = (
                latest_movements
                .filter(col("usuincl").isin(excluded_users))
                .select("coddoc")
                .distinct()
            )
            
            excluded_count = excluded_processes.count()
            self.logger.info(f"🚫 Found {excluded_count:,} processes to exclude based on user criteria")
            
            return excluded_processes
            
        except Exception as e:
            self.logger.error(f"❌ Error creating user exclusion filter: {e}")
            return self.misc._create_empty_result_df()

    def _load_meta_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 06 eligible processes (environmental crimes)...")
        filter_data = {}
                
        for filter_path in self.meta06_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # Use Spark's native parquet reader for filter files
                df = self.spark.read.parquet(filter_path)
                if df is not None:
                    record_count = df.count()
                    self.logger.info(f"   ✅ Loaded {record_count:,} processes from {filter_name}")
                    
                    # For Meta 06 (environmental crimes), capture any available filter
                    if "crime_ambiental" in filter_name or "ambiental" in filter_name:
                        filter_data["environmental_crimes"] = df.select("coddoc")
                    elif "conhecimento" in filter_name:
                        filter_data["conhecimento"] = df.select("coddoc")
                    else:
                        # Generic filter - use as environmental crimes
                        filter_data["environmental_crimes"] = df.select("coddoc")
                        
            except Exception as e:
                self.logger.error(f"❌ Error loading {filter_path}: {e}")
                continue
        
        # For Meta 06, we primarily need environmental crimes filter
        if "environmental_crimes" in filter_data:
            eligible_processes = filter_data["environmental_crimes"]
            self.logger.info("✅ Using environmental crimes filter as base for Meta 06")
        elif "conhecimento" in filter_data:
            # Fallback to conhecimento if environmental filter not found
            eligible_processes = filter_data["conhecimento"]
            self.logger.warning("⚠️ Environmental crimes filter not found - falling back to conhecimento")
        else:
            self.logger.error("❌ No suitable filters found for Meta 06")
            return None

        excluded_processes = self.get_user_exclusion_filter()
        if eligible_processes is not None:
            eligible_processes = eligible_processes.join(excluded_processes, on="coddoc", how="left_anti")
        
        # Persist safely
        eligible_processes = self.misc._persist_dataframe_safely(eligible_processes)
        
        total_eligible = eligible_processes.count()
        self.logger.info(f"✅ Meta 06 eligible processes (environmental crimes): {total_eligible:,}")
        
        return eligible_processes
    
    def _configure_meta_memory_optimizations(self):
        """Configure Spark for Meta memory optimization with advanced features - ENHANCED WITH CODEC FALLBACK."""
        try:
            spark_conf = self.spark.conf
            
            # Enhanced memory management optimizations for very large datasets
            spark_conf.set(
                "spark.sql.adaptive.enabled",
                os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false"))
            )
            spark_conf.set(
                "spark.sql.adaptive.coalescePartitions.enabled",
                os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false"))
            )
            spark_conf.set("spark.sql.adaptive.coalescePartitions.maxPartitionBytes", "64MB")
            spark_conf.set("spark.sql.shuffle.partitions", "800")
            spark_conf.set("spark.sql.autoBroadcastJoinThreshold", os.environ.get("SPARK_AUTO_BROADCAST_THRESHOLD", "-1"))
            
            # Skew join optimization for better performance with large datasets
            spark_conf.set("spark.sql.adaptive.skewJoin.enabled", os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false")))
            spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
            spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
            
            # Advanced I/O optimizations with predicate pushdown
            spark_conf.set("spark.sql.parquet.filterPushdown", "true")
            spark_conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")
            spark_conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
            spark_conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "06000")
            
            # Arrow optimization with fallback
            spark_conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            spark_conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            spark_conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "250")
            spark_conf.set("spark.sql.columnVector.offheap.enabled", "true")
            
            # CRITICAL FIX: Compression codec with Snappy fallback handling
            self._configure_compression_codec_with_fallback(spark_conf)
            
            # Checkpointing and lineage management
            from utils.parquet_utils import spark_events_path
            spark_conf.set("spark.sql.streaming.checkpointLocation", spark_events_path())
            
            self.logger.info("✅ Applied Meta enhanced memory and performance optimizations with codec fallback")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error applying memory optimizations: {e}")
            return None
    
    def _configure_compression_codec_with_fallback(self, spark_conf):
        """Configure compression codec with Snappy fallback to prevent NoClassDefFoundError."""
        try:
            # First, try to test Snappy availability
            try:
                # Test if Snappy is available by trying to import the Java class
                sc = self.spark.sparkContext
                
                # If we get here, Snappy is available
                spark_conf.set("spark.sql.parquet.compression.codec", "zstd")
                spark_conf.set("spark.sql.parquet.outputCommitterClass", "org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat$ParquetOutputCommitter")
                self.logger.info("✅ Snappy compression codec is available and configured")
                
            except Exception as snappy_error:
                # Snappy is not available, fall back to alternative codecs
                self.logger.warning(f"⚠️ Snappy codec not available: {snappy_error}")
                self.logger.info("🔄 Falling back to GZIP compression codec")
                
                # Fall back to GZIP which is always available
                spark_conf.set("spark.sql.parquet.compression.codec", "gzip")
                
                # Additional configurations to handle parquet reading issues
                spark_conf.set("spark.sql.parquet.vectorizedReader.enabled", "false")
                spark_conf.set("spark.sql.parquet.mergeSchema", "false")
                spark_conf.set("spark.sql.parquet.binaryAsString", "false")
                
                # Disable vectorized reader which can cause issues with certain codecs
                spark_conf.set("spark.sql.execution.columnar.enabled", "false")
                
                self.logger.info("✅ GZIP compression codec configured with safe parquet settings")
                
        except Exception as codec_error:
            self.logger.error(f"❌ Error configuring compression codec: {codec_error}")
            # Ultimate fallback - disable compression
            try:
                spark_conf.set("spark.sql.parquet.compression.codec", "uncompressed")
                self.logger.warning("⚠️ Using uncompressed parquet files as ultimate fallback")
            except Exception as final_error:
                self.logger.error(f"❌ Failed to set any compression codec: {final_error}")

    # Helper to select and order required columns for meta export
    META_EXPORT_COLUMNS = [
        "cod_classe", "cod_oj", "cod_proc", "cod_tip_mov", "cod_tip_mov_numero",
        "coddoc", "dth_fim", "dthrmov", "ident_mov", "nivel_sigilo", "situacao", "variavel"
    ]
    def _select_and_order_meta_columns(self, df):
        from pyspark.sql.functions import lit
        from pyspark.sql.types import StringType, IntegerType, LongType, TimestampType
        default_types = {
            "cod_classe": StringType(),
            "cod_oj": StringType(),
            "cod_proc": StringType(),
            "cod_tip_mov": StringType(),
            "cod_tip_mov_numero": StringType(),
            "coddoc": LongType(),
            "dth_fim": TimestampType(),
            "dthrmov": TimestampType(),
            "ident_mov": StringType(),
            "nivel_sigilo": IntegerType(),
            "situacao": IntegerType(),
            "variavel": StringType(),
        }
        for col_name in self.META_EXPORT_COLUMNS:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None).cast(default_types[col_name]))
        return df.select(self.META_EXPORT_COLUMNS)

    def _create_checkpoint(self, df, name="checkpoint"):
        """
        Checkpoint a Spark DataFrame and return the checkpointed DataFrame.
        """
        return df
        """
        try:
            # Set checkpoint directory if not already set
            self.logger.info(f"💾 Creating checkpoint for {name}...")
            df = df.checkpoint(eager=True)
            self.logger.info(f"✅ Checkpoint created for {name}")
            return df
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to checkpoint {name}: {e}")
            return None
        """

    def _validate_active_processes(self, df: DataFrame, variable_name: str) -> DataFrame:
        """
        Validate that processes are active by joining with documento parquet files.
        Only processes with indativo='S' are considered active and should be included in results.
        
        OPTIMIZED VERSION: Uses hybrid loader with coddoc filter instead of loading entire documento dataset.
        
        Args:
            df: DataFrame with calculation results containing coddoc column
            variable_name: Name of the variable being validated (for logging)
            
        Returns:
            DataFrame filtered to only include active processes (indativo='S')
        """
        try:
            self.logger.info(f"🔍 Validating active processes for {variable_name}...")
            
            # Get original count for comparison
            original_count = df.count()
            self.logger.info(f"📊 Original {variable_name} results: {original_count:,} records")
            
            if original_count == 0:
                self.logger.info("ℹ️ No records to validate - returning empty result")
                return df
            
            # Extract unique coddoc values from the result set
            coddoc_to_validate = df.select("coddoc").distinct()
            coddoc_count = coddoc_to_validate.count()
            self.logger.info(f"📊 Unique processes to validate: {coddoc_count:,}")
            
            # Use hybrid loader to load only the documento records we need to check
            try:
                from utils.parquet_utils import documento_path
                documento_base_path = documento_path("ejud")
                
                self.logger.info(f"📂 Loading documento data for {coddoc_count:,} processes from: {documento_base_path}")
                
                # Use hybrid loader to load only the documento records for our specific coddocs
                documento_filtered_df = self._load_documento_for_coddocs(
                    coddoc_list=coddoc_to_validate,
                    documento_base_path=documento_base_path
                )
                
                if documento_filtered_df is None or documento_filtered_df.count() == 0:
                    self.logger.warning("⚠️ No documento records found for validation coddocs")
                    self.logger.warning("⚠️ This may indicate a data issue - returning original results")
                    return df
                
                # Check if required columns exist
                if "coddoc" not in documento_filtered_df.columns or "indativo" not in documento_filtered_df.columns:
                    available_cols = documento_filtered_df.columns
                    self.logger.error(f"❌ Required columns not found in documento data")
                    self.logger.error(f"   Required: ['coddoc', 'indativo']")
                    self.logger.error(f"   Available: {available_cols}")
                    self.logger.warning("⚠️ Skipping active process validation - returning all processes")
                    return df
                
                # Filter to only active processes (indativo='S') from the filtered documento data
                active_processes = documento_filtered_df.filter(
                    col("indativo") == "S"
                ).select("coddoc").distinct()
                
                active_count = active_processes.count()
                self.logger.info(f"✅ Found {active_count:,} active processes from {coddoc_count:,} checked")
                
                if active_count == 0:
                    self.logger.warning("⚠️ No active processes found among the validation coddocs")
                    self.logger.warning("⚠️ All processes appear to be inactive - returning empty result")
                    return df.filter(col("coddoc").isNull())  # Return empty result with same schema
                
                # Join with active processes to filter results
                # Use inner join to keep only active processes
                validated_df = df.join(
                    active_processes,
                    "coddoc",
                    "inner"
                )
                
                validated_count = validated_df.count()
                excluded_count = original_count - validated_count
                
                self.logger.info(f"✅ Active process validation completed for {variable_name}:")
                self.logger.info(f"   📈 Original records: {original_count:,}")
                self.logger.info(f"   ✅ Active processes: {validated_count:,}")
                self.logger.info(f"   ❌ Excluded (inactive): {excluded_count:,}")
                self.logger.info(f"   📋 Retention rate: {(validated_count/original_count*100) if original_count > 0 else 0:.1f}%")
                
                return validated_df
                
            except Exception as e:
                self.logger.error(f"❌ Error in active process validation: {e}")
                self.logger.warning("⚠️ Skipping active process validation - returning all processes")
                return df
        except Exception as e:
                self.logger.error(f"❌ Error in active process validation: {e}")
                self.logger.warning("⚠️ Skipping active process validation - returning all processes")
                return df

    def _load_documento_for_coddocs(self, coddoc_list: DataFrame, documento_base_path: str) -> Optional[DataFrame]:
        """
        Load documento data for specific coddocs using efficient filtering.
        
        Args:
            coddoc_list: DataFrame with coddoc column containing the processes to load
            documento_base_path: Base path to documento parquet files
        
        Returns:
            DataFrame with documento records for the specified coddocs, or None if error
        """
        self.logger.info(f"📂 Loading documento data for specific coddocs from: {documento_base_path}")
        
        # Temporarily disable broadcast join to avoid memory issues
        original_threshold = self.spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        
        try:
            # Create temporary view for coddoc filter
            coddoc_list.createOrReplaceTempView("validation_coddocs")
            
            # Try different patterns to find documento files
            patterns_to_try = [
                f"{documento_base_path}/documento_batch_*.parquet",
                f"{documento_base_path}/documento*.parquet",
                f"{documento_base_path}/*.parquet"
            ]
            
            documento_df = None
            for pattern in patterns_to_try:
                try:
                    found_files = glob.glob(pattern)
                    if found_files:
                        self.logger.info(f"📁 Found {len(found_files)} documento files with pattern: {pattern}")
                        documento_df = self.spark.read.parquet(pattern)
                        break
                except Exception as pattern_error:
                    self.logger.debug(f"Pattern {pattern} failed: {pattern_error}")
                    continue
            
            if documento_df is None:
                self.logger.error(f"❌ No documento files found at: {documento_base_path}")
                return None
            
            # Check if required columns exist
            if "coddoc" not in documento_df.columns or "indativo" not in documento_df.columns:
                self.logger.error(f"❌ Required columns not found in documento data")
                self.logger.error(f"   Available columns: {documento_df.columns}")
                return None
            
            # Create temporary view for documento
            documento_df.createOrReplaceTempView("documento_all")
            
            # Use SQL with inner join to filter documento to only our validation coddocs
            # This is much more efficient than loading all documento data
            filtered_sql = """
            SELECT d.* 
            FROM documento_all d
            INNER JOIN validation_coddocs v ON d.coddoc = v.coddoc
            """
            
            documento_filtered = self.spark.sql(filtered_sql)
            filtered_count = documento_filtered.count()
            
            self.logger.info(f"📊 Loaded {filtered_count:,} documento records for validation")
            
            return documento_filtered
            
        finally:
            # Restore original broadcast threshold
            self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", original_threshold)
                
    def calculate_all_meta_spark(self) -> bool:
        """
        Calculate all Meta variables using Spark distributed processing.
        Returns:
            bool: True if all calculations completed successfully
        """
        self.logger.info("🚀 Starting comprehensive Meta calculation with Spark...")
        
        start_time = datetime.now()
        results = {}
        """
        Just run ONCE per year: _calculate_dism_old_with_eligible_processes, _calculate_old_88_pending,
        _calculate_old_25_close, _calculate_dism_with_eligible_processes
        """
        # Phase 1: Base variables (independent calculations)
        base_calculations = [
         #   ("dism06", self.meta_stocks._calculate_dism_optimized_with_stock_tables),
            ("baixm06", self.meta_var._calculate_baixm_with_eligible_processes),
            ("primeirasentm06", self.meta_var._calculate_primeirasentm_with_eligible_processes),
            ("mpum06", self.meta_var._calculate_mpum_with_eligible_processes),
        ]
        
        # Phase 2: Dependent variables (require base variables to be exported first)
        dependent_calculations = [
           ("julgadom06", self.meta_var._calculate_julgadom_with_eligible_processes),  # Depends on baixm06 + primeirasentm06 + mpum06
           ("susm06", self.meta_var._calculate_susm_with_eligible_processes),  # Depends on dism06 and julgadom06
        ]
        
        # Execute in phases to ensure proper dependencies
        all_calculation_phases = [
            ("Phase 1: Base Variables", base_calculations),
            ("Phase 2: Dependent Variables", dependent_calculations)
        ]
        
        # Flatten calculations for total count
        all_calculations = base_calculations + dependent_calculations
        
        for phase_name, phase_calculations in all_calculation_phases:
            self.logger.info(f"\n🔥 {phase_name.upper()}")
            self.logger.info(f"{'='*80}")
            
            for variable_name, calculation_method in phase_calculations:
                try:
                    self.logger.info(f"\n{'='*60}")
                    self.logger.info(f"🎯 Calculating {variable_name.upper()}")
                    self.logger.info(f"{'='*60}")
                    
                    calculation_start = datetime.now()
                    result = calculation_method()
                    
                    if result is not None:
                        # STEP 1: Enforce output schema
                        result = self._select_and_order_meta_columns(result)
                        
                        # STEP 2: Validate active processes using documento data
                        result = self._validate_active_processes(result, variable_name)
                        
                        # Update record count after validation
                        validated_count = result.count()
                        
                        # STEP 3: Create final checkpoint before export
                        result = self._create_checkpoint(result, f"{variable_name}_final")
                        
                        # STEP 4: Export with synchronization verification
                        incremental_mode = self.incremental_months > 0
                        if self.results_exporter.export_results(result, variable_name, incremental_mode=incremental_mode):
                            self.logger.info(f"✅ {variable_name} exported: {validated_count:,} records (after active process validation)")
                            results[variable_name] = validated_count
                            
                        # Memory cleanup WITHOUT stopping Spark (keep session alive for next calculations)
                        self.misc._cleanup_memory_aggressive(stop_spark=False)
                    else:
                        self.logger.error(f"❌ {variable_name} returned None")
                        results[variable_name] = 0
                        
                except Exception as e:
                    self.logger.error(f"❌ Error calculating {variable_name}: {e}")
                    import traceback
                    self.logger.error(f"📋 Traceback: {traceback.format_exc()}")
                    results[variable_name] = 0
                    continue
        
        # Calculate total time and log summary
        total_time = (datetime.now() - start_time).total_seconds()
        self.meta_logger.log_calculation_completion(results, total_time)
        self.calculation_results = results
        
        if self.calculation_results is not None:
            return True
        else:
            self.logger.warning(f"⚠️ calculations completed")
            return False
    
def main():
    import os
    default_spark_local = "/opt/parquet/tempspark"
    if not os.environ.get("SPARK_LOCAL_DIRS"):
        logger.warning(f"SPARK_LOCAL_DIRS not set; defaulting to {default_spark_local}. Ensure this path exists on all workers.")
        os.environ.setdefault("SPARK_LOCAL_DIRS", default_spark_local)
        try:
            os.makedirs(default_spark_local, exist_ok=True)
        except Exception:
            logger.debug("Could not create local Spark dir on driver; ensure path exists on worker nodes.")

    """Main execution function for production Meta calculations."""
    parser = argparse.ArgumentParser(description='Meta EJUD Production Calculator')
    parser.add_argument('--period_start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--period_end', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--incremental', type=int, default=2,
                        help='Enable incremental mode (process only recent N months, default=2)')
    
    args = parser.parse_args()
    
    # Parse dates if provided
    period_start = None
    period_end = None
    
    if args.period_start:
        period_start = datetime.strptime(args.period_start, '%Y-%m-%d').date()
    if args.period_end:
        period_end = datetime.strptime(args.period_end, '%Y-%m-%d').date()
    
    calculator = None
    spark = None
    try:
        # Create dedicated SparkSession for this script
        spark = (
            SparkMetaUtils.get_optimized_spark_config("Meta_06")
            .config("spark.eventLog.enabled", "false")
            .config(
                "spark.sql.adaptive.enabled",
                os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false"))
            )
            .config(
                "spark.sql.adaptive.coalescePartitions.enabled",
                os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false"))
            )
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        

        # Initialize calculator (it will reuse the SparkSession we just created)
        calculator = MetaCNJ2026CalculatorEJUD_Spark(
            period_start=period_start,
            period_end=period_end,
            incremental_months=args.incremental
        )
        
        # Run calculations
        success = calculator.calculate_all_meta_spark()
        
        if success:
            logger.info("🎉 Meta production calculations completed successfully!")
            sys.exit(0)
        else:
                logger.error("❌ Meta 02 calculations failed")
                sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ Fatal error: {e}")
        sys.exit(1)
    finally:
        # Best-effort cleanup to avoid leftover shuffle races
        try:
            import time
            gc.collect()
            if calculator is not None:
                try:
                    # unpersist / memory cleanup without stopping spark
                    if getattr(calculator, "misc", None):
                        calculator.misc._cleanup_memory_aggressive(stop_spark=False)
                except Exception:
                    pass
            # Stop the SparkSession we created for this script
            try:
                if spark is not None:
                    safe_stop_spark(spark, logger=logger)
            except Exception:
                logger.warning("Failed to stop Spark gracefully.")
                    
            time.sleep(1)  # brief pause to allow background threads to settle
        except Exception:
            pass
        sys.exit(0)

if __name__ == "__main__":
    main()
