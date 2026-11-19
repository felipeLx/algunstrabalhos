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
from utils.misc import MetaMiscellaneous
from utils.hybrid_parquet_loader import HybridParquetLoader
from utils.spark_meta_utils import SparkMetaMixin, SparkMetaUtils
from utils.meta_results_exporter import MetaResultsExporter
from utils.meta_logging_system import MetaLogger
from utils.meta_config_manager import MetaConfigManager
from utils.misc import safe_stop_spark

# PySpark imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, count, sum as spark_sum,
        max as spark_max, min as spark_min, first, desc, asc,
        year, month, dayofmonth, to_date, date_format,
        coalesce, current_timestamp, concat_ws, 
        row_number, rank, dense_rank
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructSchema, StructField, StringType, IntegerType, 
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

class MetaCNJ2025CalculatorEJUD_Meta02_Production(SparkMetaMixin):
    
    def __init__(self, app_name: str = "MetaCNJ2025_Meta02_Spark", use_recovery: bool = True,
                 period_start: Optional[date] = None, period_end: Optional[date] = None,
                 spark: "SparkSession" = None,
                 config_manager: "MetaConfigManager" = None,
                 logger: logging.Logger = None,
                 misc: "MetaMiscellaneous" = None,
                 hybrid_loader: "HybridParquetLoader" = None,
                 results_exporter: "MetaResultsExporter" = None,
                 incremental_months: int = 0):
        """Initialize Spark calculator with optimized configuration for Meta 02."""
        
        # Store incremental mode setting
        self.incremental_months = incremental_months
        
        # Initialize configuration manager (use injected if provided)
        self.config_manager = config_manager if config_manager is not None else MetaConfigManager()
        self.meta_number = "02"
        
        # Always instantiate MetaLogger; if an external logger is provided inject it
        self.meta_logger = MetaLogger("02")
        if logger is not None:
            self.logger = logger
            try:
                self.meta_logger.logger = self.logger
            except Exception:
                pass
        else:
            self.logger = self.meta_logger.logger
            self.meta_logger.log_calculation_start("Meta 02 CNJ 2025 Spark Calculator - Multi-Filter Implementation")

        # If a SparkSession is injected, reuse it; otherwise create one
        self._shared_spark = spark is not None
        if spark is not None:
            self.spark = spark
            self.logger.info("🔁 Using injected shared SparkSession")
        else:
            self.spark = SparkMetaUtils.get_optimized_spark_config(app_name)

        # Suppress some common Spark warnings during cleanup
        try:
            spark_logger = self.spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark.storage.BlockManagerStorageEndpoint")
            spark_logger.setLevel(self.spark.sparkContext._jvm.org.apache.log4j.Level.WARN)
        except Exception:
            pass

        # Initialize results_exporter (reuse injected one if present)
        if results_exporter is not None:
            self.results_exporter = results_exporter
        else:
            self.results_exporter = MetaResultsExporter(self.spark, "02", config_manager=self.config_manager)

        # Load paths from configuration
        self.ejud_base_path = self.config_manager.get_system_path("ejud_base_path")
        try:
            self.situacao_base_path = self.config_manager.get_system_path("situacao_base_path")
        except Exception:
            # fallback to parquet utils
            try:
                from utils.parquet_utils import ejud_situacao_base
                self.situacao_base_path = ejud_situacao_base()
            except Exception:
                self.situacao_base_path = None
        self.use_recovery = use_recovery
        
        # Load date configuration from JSON
        self.period_end = self.config_manager.get_date_cutoff("meta_02", "period_end")
        self.period_start = self.config_manager.get_date_cutoff("meta_02", "period_start")
        self.meta02_15year_cutoff = self.config_manager.get_date_cutoff("meta_02", "meta02_15year_cutoff")        
        self.meta02_cutoff_date = self.config_manager.get_date_cutoff("meta_02", "meta02_cutoff_date")
        
          # Load Meta 02 filter paths
        self.meta02_filter_paths = self.config_manager.get_meta_path("meta_02", "meta02_filter_paths")
        
        # Initialize or reuse misc/hybrid_loader
        if misc is not None:
            self.misc = misc
        else:
            self.misc = MetaMiscellaneous(spark=self.spark, logger=self.logger, meta_number="02")

        if hybrid_loader is not None:
            self.hybrid_loader = hybrid_loader
        else:
            self.hybrid_loader = HybridParquetLoader(
                spark_session=self.spark, 
                logger=self.logger, 
                misc=self.misc, 
                apply_meta03_filter=False,
                incremental_months=self.incremental_months
            )
        
        # Initialize eligible processes after hybrid loader is ready
        self.eligible_processes_df = self._load_meta02_eligible_processes()
        
        # Load available situacoes for validation
        self.available_situacoes = self.config_manager.get_available_situacoes("ejud")
        
        # Apply Meta 02 specific memory optimizations
        self._configure_meta02_memory_optimizations()
        
        # Initialize Meta02Stocks instance
        self.meta02_stocks = MetaCalculator(
            spark=self.spark,
            config_manager=self.config_manager,
            logger=self.logger,
            meta_path=self.meta02_filter_paths,
            meta_logger=self.meta_logger,
            hybrid_loader=self.hybrid_loader,
            misc=self.misc,
            eligible_processes_df=self.eligible_processes_df,
            period_start=self.period_start,
            period_end=self.period_end,
            meta_cutoff_date=self.meta02_cutoff_date,
            meta_15year_cutoff=self.meta02_15year_cutoff,
            meta_number=self.meta_number
        )

        # Initialize Meta02 Variables
        self.meta02_var = MetaVariables(
            spark=self.spark,
            config_manager=self.config_manager,
            logger=self.logger,
            meta_path = self.meta02_filter_paths,
            meta_logger=self.meta_logger,
            hybrid_loader=self.hybrid_loader,
            misc = self.misc,
            eligible_processes_df = self.eligible_processes_df,
            period_start=self.period_start,
            period_end=self.period_end,
            meta_cutoff_date=self.meta02_cutoff_date,
            meta_15year_cutoff=self.meta02_15year_cutoff,
            meta_number=self.meta_number
        )
        
        self.logger.info(f"📅 Meta 02 date configuration:")
        self.logger.info(f"   📊 Period: {self.period_start} to {self.period_end}")
        self.logger.info(f"   📂 Filter files: {len(self.meta02_filter_paths)} files")
        self.logger.info(f"   📋 Available situacoes: {len(self.available_situacoes)} codes")
        
        # Store results for summary
        self.calculation_results = {}

    def _load_meta02_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 02 eligible processes using intersection logic...")
        filter_data = {}
                
        for filter_path in self.meta02_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # Use glob pattern helper to avoid reading metadata files
                df = self.spark.read.parquet(filter_path)
                if df is not None:
                    # Check if this is the dism02 result file
                    if "filtered_coddoc_classe_conhecimento" in filter_name:
                        filter_data["filtered_coddoc_classe_conhecimento"] = df.select("coddoc")
                        
            except Exception as e:
                self.logger.error(f"❌ Error loading {filter_path}: {e}")
                continue
        
           
        if "filtered_coddoc_classe_conhecimento" not in filter_data:
            self.logger.error("❌ Missing required filtered_coddoc_classe_conhecimento filter")
            return None
        
        #conhecimento_processes = filter_data["conhecimento"]
        
        # Union filtered_coddoc_classe_conhecimento
        if "filtered_coddoc_classe_conhecimento" in filter_data:
            public_processes = filter_data["filtered_coddoc_classe_conhecimento"]
        
        # Final intersection
        eligible_processes = public_processes

        # Persist safely
        eligible_processes = self.misc._persist_dataframe_safely(eligible_processes)
        
        self.logger.info(f"✅ Meta 02 eligible processes (from intersection)")
        
        return eligible_processes

    def _configure_meta02_memory_optimizations(self):
        """Configure Spark for Meta 02 memory optimization with advanced features - ENHANCED WITH CODEC FALLBACK."""
        try:
            spark_conf = self.spark.conf
            # Enhanced memory management optimizations for very large datasets
            spark_conf.set("spark.sql.adaptive.enabled", "true")
            spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark_conf.set("spark.sql.adaptive.coalescePartitions.maxPartitionBytes", "64MB")
            spark_conf.set("spark.sql.shuffle.partitions", "800")
            spark_conf.set("spark.sql.autoBroadcastJoinThreshold", "32MB")
            
            # Skew join optimization for better performance with large datasets
            spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
            spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
            
            # Advanced I/O optimizations with predicate pushdown
            spark_conf.set("spark.sql.parquet.filterPushdown", "true")
            spark_conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")
            spark_conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
            spark_conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
            
            spark_conf.set("spark.sql.parquet.compression.codec", "zstd")
            
            self.logger.info("✅ Applied Meta 02 enhanced memory and performance optimizations with codec fallback")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error applying memory optimizations: {e}")
            return None

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
            "cod_tip_mov": IntegerType(),
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
        try:
            self.logger.info(f"🔍 Validating active processes for {variable_name}...")
            
            # Extract unique coddoc values from the result set
            coddoc_to_validate = df.select("coddoc").distinct()
            self.logger.info(f"📊 Unique processes to validate")
            
            # Use hybrid loader to load only the documento records we need to check
            try:
                from utils.parquet_utils import documento_path
                documento_base_path = documento_path("ejud")
                
                # Use hybrid loader to load only the documento records for our specific coddocs
                documento_filtered_df = self._load_documento_for_coddocs(
                    coddoc_list=coddoc_to_validate,
                    documento_base_path=documento_base_path
                )
                
                if documento_filtered_df is None:
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
                
                
                if active_processes is None:
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
                                
                self.logger.info(f"✅ Active process validation completed for {variable_name}:")
                
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
            filtered_sql = """
            SELECT d.* 
            FROM documento_all d
            INNER JOIN validation_coddocs v ON d.coddoc = v.coddoc
            """
            
            documento_filtered = self.spark.sql(filtered_sql)
            
            return documento_filtered
        
        except Exception as e:
            self.logger.error(f"❌ Error loading documento data: {e}")
            return None
            
    def calculate_all_meta02_spark(self) -> bool:
        """
            bool: True if all calculations completed successfully
        """
        self.logger.info("🚀 Starting comprehensive Meta 02 calculation with Spark...")
        
        start_time = datetime.now()
        results = {}
        # Phase 1: Base variables (independent calculations) - AUTO-OPTIMIZED with stock tables
        base_calculations = [
        #    ("dism02_ant", self.meta02_stocks._calculate_dism_old_optimized_with_stock_tables), # _calculate_dism_old_with_eligible_processes
        #    ("dism02", self.meta02_stocks._calculate_dism_optimized_with_stock_tables),
        #    ("pend_old_88", self.meta02_stocks._calculate_old_88_pending),
        #    ("pend_old_25", self.meta02_stocks._calculate_old_25_close),
            ("baixm02", self.meta02_var._calculate_baixm_with_eligible_processes),
            ("primeirasentm02", self.meta02_var._calculate_primeirasentm_with_eligible_processes),
            ("mpum02", self.meta02_var._calculate_mpum_with_eligible_processes),
        ]
        
        # Phase 2: Dependent variables (require base variables to be exported first)
        dependent_calculations = [
           ("julgadom02", self.meta02_var._calculate_julgadom_with_eligible_processes),  # Depends on baixm02 + primeirasentm02 + mpum02
           ("julgadom02_ant", self.meta02_var._calculate_julgadom_ant),  # Historical version
           ("susm02", self.meta02_var._calculate_susm_with_eligible_processes),  # Depends on dism02 and julgadom02
           ("susm02_ant", self.meta02_var._calculate_susm_ant),  # Historical version
           ("desm02_ant", self.meta02_var._calculate_desm_ant_with_eligible_processes),  # Depends on susm02_ant
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
                        
                        
                        # STEP 3: Create final checkpoint before export
                        result = self._create_checkpoint(result, f"{variable_name}_final")
                        
                        # STEP 4: Export with synchronization verification
                        incremental_mode = self.incremental_months > 0
                        if self.results_exporter.export_results(result, variable_name, incremental_mode=incremental_mode):
                            self.logger.info(f"✅ {variable_name} exported:records (after active process validation)")
                        # Memory cleanup WITHOUT stopping Spark (keep session alive for next calculations)
                        self.misc._cleanup_memory_aggressive(stop_spark=False)
                    else:
                        self.logger.error(f"❌ {variable_name} returned None")
                        results[variable_name] = 0
                        
                except Exception as e:
                    self.logger.error(f"❌ Error calculating {variable_name}: {e}")
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
    """Main execution function for production Meta 02 calculations."""
    parser = argparse.ArgumentParser(description='Meta 02 EJUD Production Calculator')
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
    import os
    try:
        # Create dedicated SparkSession for this script
        spark = (
            SparkMetaUtils.get_optimized_spark_config("Meta_02")
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
        

        # Initialize calculator with the dedicated SparkSession
        calculator = MetaCNJ2025CalculatorEJUD_Meta02_Production(
            period_start=period_start,
            period_end=period_end,
            incremental_months=args.incremental,
            spark=spark
        )
        
        # Run calculations
        success = calculator.calculate_all_meta02_spark()
        
        if success:
            logger.info("🎉 Meta 02 production calculations completed successfully!")
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
