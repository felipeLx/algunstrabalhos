from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import argparse
import sys
import os
import logging
import glob
import gc

from variables.variables_calculator import MetaVariables


# Import enhanced utilities from utils module
from stocks.meta_stocks import MetaCalculator
from utils.hybrid_parquet_loader import HybridParquetLoader
from utils.misc import MetaMiscellaneous, safe_stop_spark
from utils.spark_meta_utils import SparkMetaMixin, SparkMetaUtils
from utils.meta_results_exporter import MetaResultsExporter
from utils.meta_logging_system import MetaLogger
from utils.meta_config_manager import MetaConfigManager

import tempfile
import shutil

# PySpark imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, sum as spark_sum,
        max as spark_max, min as spark_min, first, desc, asc,
        year, month, dayofmonth, to_date, date_format,
        coalesce, current_timestamp, concat_ws, 
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

class MetaCNJ2025CalculatorEJUD_Meta01_Production(SparkMetaMixin):
    def __init__(self, app_name: str = "MetaCNJ2025_Meta01_Spark", use_recovery: bool = True,
                 period_start: Optional[date] = None, period_end: Optional[date] = None, spark: "SparkSession" = None,
                 config_manager: "MetaConfigManager" = None,
                 logger: logging.Logger = None,
                 misc: "MetaMiscellaneous" = None,
                 hybrid_loader: "HybridParquetLoader" = None,
                 results_exporter: "MetaResultsExporter" = None,
                 incremental_months: int = 0):
        """Initialize Spark calculator with optimized configuration for Meta 01."""
        
        # Store incremental mode setting
        self.incremental_months = incremental_months
        
        # If a config_manager is provided, use it; otherwise create one like before
        self.config_manager = config_manager if config_manager is not None else MetaConfigManager()

        # Always instantiate MetaLogger so its helper methods are available.
        # If an external logger was provided, inject it into MetaLogger so both
        # use the same handlers/stream.
        self.meta_logger = MetaLogger("01")
        if logger is not None:
            self.logger = logger
            try:
                # prefer the provided logger for MetaLogger output when possible
                self.meta_logger.logger = self.logger
            except Exception:
                # fall back silently if MetaLogger internals differ
                pass
        else:
            self.logger = self.meta_logger.logger
            self.meta_logger.log_calculation_start("Meta 01 CNJ 2026 Spark Calculator - Multi-Filter Implementation")

        # If a SparkSession is injected, reuse it; otherwise create one (existing flow)
        self._shared_spark = spark is not None
        if spark is not None:
            self.spark = spark
            self.logger.info("🔁 Using injected shared SparkSession")
        else:
            self.spark = SparkMetaUtils.get_optimized_spark_config(app_name)
            # to avoid confusing ERROR entries in user logs.
            try:
                spark_logger = self.spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark.storage.BlockManagerStorageEndpoint")
                spark_logger.setLevel(self.spark.sparkContext._jvm.org.apache.log4j.Level.WARN)
            except Exception:
                pass
            
        # If misc/hybrid_loader were injected, use them; otherwise create them
        if misc is not None:
            self.misc = misc
        else:
            self.misc = MetaMiscellaneous(spark=self.spark, logger=self.logger, meta_number="01")

        if hybrid_loader is not None:
            self.hybrid_loader = hybrid_loader
        else:
            self.hybrid_loader = HybridParquetLoader(
                self.spark, 
                self.logger, 
                self.misc, 
                apply_meta03_filter=False,
                incremental_months=self.incremental_months
            )
        
        if results_exporter is not None:
            self.results_exporter = results_exporter
        else:
            self.results_exporter = MetaResultsExporter(self.spark, "01", config_manager=self.config_manager)
        # Load situacao base path from configuration (used by hybrid loader calls)
        try:
            self.situacao_base_path = self.config_manager.get_system_path("situacao_base_path")
        except Exception:
            # Fallback to default parquet utils path
            try:
                from utils.parquet_utils import ejud_situacao_base
                self.situacao_base_path = ejud_situacao_base()
            except Exception:
                self.situacao_base_path = None
        
        # Load date configuration from JSON
        self.period_end = self.config_manager.get_date_cutoff("meta_01", "reference_date")
        self.period_start = self.config_manager.get_date_cutoff("meta_01", "period_start")
        self.meta01_15year_cutoff = self.config_manager.get_date_cutoff("meta_01", "meta01_15year_cutoff")        
        self.meta01_cutoff_date = self.config_manager.get_date_cutoff("meta_01", "meta01_cutoff_date")
        
          # Load Meta 01 filter paths
        self.meta01_filter_paths = self.config_manager.get_meta_path("meta_01", "meta01_filter_paths")
        
        # Initialize eligible processes after hybrid loader is ready
        self.eligible_processes_df = self._load_meta01_eligible_processes()
        
        # Load available situacoes for validation
        self.available_situacoes = self.config_manager.get_available_situacoes("ejud")
        
        # Apply Meta 01 specific memory optimizations
        self._configure_meta01_memory_optimizations()
        
        self.logger.info(f"📅 Meta 01 date configuration:")
        self.logger.info(f"   📊 Period: {self.period_start} to {self.period_end}")
        self.logger.info(f"   📂 Filter files: {len(self.meta01_filter_paths)} files")
        self.logger.info(f"   📋 Available situacoes: {len(self.available_situacoes)} codes")

        # Store results for summary
        self.calculation_results = {}

    def _load_parquet_with_glob(self, path: str) -> DataFrame:
        """Load parquet files using glob pattern to avoid metadata files and handle compression issues."""
        try:
            # Configure Spark to handle problematic parquet files
            self.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
            self.spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
            
            # Try direct read with recursive lookup (letting Spark handle nested folders)
            df = self.spark.read.option("recursiveFileLookup", "true").parquet(path)
            return df
        except Exception as direct_error:
            # Avoid referencing undefined variables on error paths; fall back
            # to a safe empty DataFrame provided by MetaMiscellaneous.
            self.logger.warning(f"⚠️ Direct parquet read failed for {path}: {direct_error}")
            try:
                # If path is a directory, ask Spark to read it with recursive lookup
                import os
                if os.path.isdir(path):
                    try:
                        self.logger.info(f"🔍 Falling back to directory read for: {path}")
                        df = self.spark.read.option("recursiveFileLookup", "true").parquet(path)
                        return df
                    except Exception as dir_err:
                        self.logger.warning(f"⚠️ Directory fallback also failed: {dir_err}")
            except Exception:
                pass
            # Final safe fallback: return an empty DataFrame shaped by the project's misc helper
            try:
                return self.misc._create_empty_result_df()
            except Exception:
                # If misc is unavailable for any reason, create a minimal empty DataFrame
                try:
                    return self.spark.createDataFrame([], schema=None)
                except Exception:
                    # Last resort: raise a controlled error so caller can handle it
                    self.logger.error("❌ Unable to create a safe empty DataFrame")
                    raise
      
    def _load_meta01_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 01 eligible processes using intersection logic...")
        filter_data = {}
                
        for filter_path in self.meta01_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # Use glob pattern helper to avoid reading metadata files
                df = self._load_parquet_with_glob(filter_path)
                    
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
        
        # Persist safely using shared misc helper
        #eligible_processes = self.misc._persist_dataframe_safely(eligible_processes, partition_by="coddoc")

        return eligible_processes

    # NOTE: Use shared persistence helper from utils.misc: self.misc._persist_dataframe_safely

    def _calculate_mpum01_with_eligible_processes(self) -> DataFrame:
        """
        Implements mpum01: Soma de ocorrências das situações Decisão proferida (17) e Medida protetiva homologada ou revogada (34),
        restrita aos movimentos 11423, 11424, 11425, 12476, 12479, 15486, 15487 e 15488, primeira ocorrência no processo no mês analisado.
        """
        from pyspark.sql.functions import (
        col, lit, current_timestamp,
        row_number
        )
        from pyspark.sql.window import Window
        
        calculation_start = datetime.now()
        self.logger.info("🚀 Calculating mpum01 (medidas protetivas e decisões proferidas no mês para Meta 01)...")
        situacao_codes =  self.config_manager.get_situacao_codes("metas", "mpum_situacoes")
        
        mpum_dfs = []
        for situacao in situacao_codes:
            situacao_df = self.load_situacao_with_coddoc_filter(
                situacao,
                apply_period_filter=True,
                period_start=self.meta01_cutoff_date,  
                period_end=self.period_end
            )
            if situacao_df is not None:
                mpum_dfs.append(situacao_df)
        
        if not mpum_dfs:
            self.logger.error("❌ No mpum situacao files loaded!")
            return None

        # Union all mpum situacoes
        all_mpums = mpum_dfs[0]
        for df in mpum_dfs[1:]:
            all_mpums = all_mpums.unionByName(df, allowMissingColumns=True)

        # Filter the movements WHERE cod_tip_mov IN  ('11423', '11424', '11425', '12476', '12479', '15486','15487','15488')
        eligible_mpums = all_mpums.filter(col("cod_tip_mov_numero").isin(self.config_manager.get_situacao_codes("metas", "valid_movement_codes")))

        # Data is already filtered by eligible processes (coddoc predicate pushdown)
        # No need for expensive join - data only contains eligible processes
        self.logger.info("✅ Data already filtered by eligible processes via predicate pushdown")
        eligible_mpums = (eligible_mpums
                         .withColumn("row_number", 
                                    row_number().over(
                                        Window.partitionBy("coddoc")
                                              .orderBy("dthrmov")
                                    ))
                          .filter(col("row_number") == 1)
                          .drop("row_number"))
        
        # Add metadata
        result_df = eligible_mpums.withColumn("variavel", lit("mpum01")) \
                                 .withColumn("meta_indicator", lit("meta_01")) \
                                 .withColumn("dth_referencia", lit(self.period_end)) \
                                 .withColumn("dth_calculo", current_timestamp()) \
                                 .withColumn("periodo_inicio", lit(self.period_start)) \
                                 .withColumn("periodo_fim", lit(self.period_end))
        
        # Optimize and persist using centralized helper
        #result_df = self.misc._persist_dataframe_safely(result_df, partition_by="situacao")

        self.logger.info(f"✅ mpum01 calculation completed ")

        return result_df
    
    def _load_constituent_variable_from_parquet(self, parquet_path: str, variable_name: str) -> DataFrame:
        """Load constituent variable from exported parquet file."""
        try:
            if not os.path.exists(parquet_path):
                self.logger.warning(f"⚠️ Parquet file not found: {parquet_path}")
                return None
                
            self.logger.info(f"📂 Loading {variable_name} from: {parquet_path}")
            df = self.spark.read.parquet(parquet_path)
            
            if df is not None:
                return df
            else:
                self.logger.warning(f"   ⚠️ Failed to load {variable_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error loading {variable_name}: {e}")
            return None

    def _calculate_julgadom01_with_eligible_processes(self) -> DataFrame:
        """
        Calculate julgadom01 (judged processes) using Meta 01 eligible processes.
        """
        from pyspark.sql.functions import (
        col, lit, current_timestamp,
        row_number, year, month
        )
        from pyspark.sql.window import Window
        
        calculation_start = datetime.now()
        self.logger.info("🚀 Calculating julgadom01...")
        
        # Collect results from constituent variables
        constituent_results = []
        from utils.parquet_utils import meta_export_path
        meta01_export_path = meta_export_path("meta_01", "ejud")
        
        # Load constituent variables from parquet
        for variable in ["primeirasentm01", "baixm01", "mpum01"]:
            parquet_path = f"{meta01_export_path}/{variable}.parquet"
            df = self._load_constituent_variable_from_parquet(parquet_path, variable)
            if df is not None:
                constituent_results.append(df)
        
        if not constituent_results:
            self.logger.error("❌ No constituent results found for julgadom01!")
            return None
        
        # Union all constituent results
        all_judged = constituent_results[0]
        for df in constituent_results[1:]:
            all_judged = all_judged.unionByName(df, allowMissingColumns=True)
        
        # Deduplicate - keep first occurrence per process
        judged_processes = (all_judged
                           .withColumn("judgment_rank", 
                                     row_number().over(
                                         Window.partitionBy("coddoc")
                                               .orderBy("dthrmov")
                                     ))
                           .filter(col("judgment_rank") == 1)
                           .drop("judgment_rank"))
        
        # Add metadata and required columns
        result_df = (judged_processes
                    .withColumn("variavel", lit("julgadom01"))
                    .withColumn("meta_indicator", lit("meta_01"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        
        self.logger.info(f"✅ julgadom01 completed records in {calculation_time:.2f}s")
        self.logger.info("📦 Exporting result DataFrame...")
        return result_df

    def _calculate_primeirasentm01_with_eligible_processes(self) -> DataFrame:
        """
        Calculate primeirasentm01 (first sentences) using Meta 01 eligible processes.
        Ensures intersection filter and date cut are applied.
        """
        from pyspark.sql.functions import (
        col, lit, current_timestamp,
        row_number, year, month
        )
        from pyspark.sql.window import Window
        
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating primeirasentm01 (first sentences for Meta 01)...")
        
        # Load sentencing situacao codes from configuration
        prim_sent_situacoes = self.config_manager.get_situacao_codes("metas", "prim_sent_situacoes")
        self.logger.info(f"📂 Loading sentencing situacoes: {prim_sent_situacoes}")
        
        sentencing_dfs = []
        for situacao in prim_sent_situacoes:
            situacao_df = self.load_situacao_with_coddoc_filter(
                situacao,
                apply_period_filter=True,
                period_start=self.meta01_cutoff_date,
                period_end=self.period_end

            )
            if situacao_df is not None:
                sentencing_dfs.append(situacao_df)
        
        if not sentencing_dfs:
            self.logger.error("❌ No sentencing situacao files loaded!")
            return None
        
        # Union all sentencing situacoes
        all_sentences = sentencing_dfs[0]
        for df in sentencing_dfs[1:]:
            all_sentences = all_sentences.unionByName(df, allowMissingColumns=True)

        self.logger.info("✅ Data already filtered by eligible processes via predicate pushdown")
        eligible_sentences = all_sentences
        
        # Get only the first sentence per process (window function)
        self.logger.info("🥇 Identifying first sentences per process...")
        window = Window.partitionBy("coddoc").orderBy("dthrmov")
        first_sentences = (eligible_sentences
                          .withColumn("rn", 
                                    row_number().over(window))
                          .filter(col("rn") == 1)
                          .drop("rn"))
        
        
        # Add metadata
        result_df = first_sentences.withColumn("variavel", lit("primeirasentm01")) \
                                 .withColumn("meta_indicator", lit("meta_01")) \
                                 .withColumn("dth_referencia", lit(self.period_end)) \
                                 .withColumn("dth_calculo", current_timestamp()) \
                                 .withColumn("periodo_inicio", lit(self.period_start)) \
                                 .withColumn("periodo_fim", lit(self.period_end))
        
        # Optimize and cache
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ primeirasentm01 calculation completed  records in {calculation_time:.2f}s")
        
        return result_df

    def _calculate_baixm01_with_eligible_processes(self) -> DataFrame:
        """
        Calculate baixm01 (archived processes) using Meta 01 eligible processes.
        Ensures intersection filter and date cut are applied.
        """
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating baixm01 (archived processes for Meta 01)...")
        from pyspark.sql.functions import (
        col, lit, current_timestamp,
        row_number, year, month
        )
        from pyspark.sql.window import Window
        
        # Load baixa situacao codes from configuration
        baixa_situacoes = self.config_manager.get_situacao_codes("metas", "baixa_situacoes")
        self.logger.info(f"📂 Loading baixa situacoes: {baixa_situacoes}")
        
        baixa_dfs = []
        for situacao in baixa_situacoes:
            situacao_df = self.load_situacao_with_coddoc_filter(
                situacao,
                apply_period_filter=True,
                period_start=self.meta01_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                baixa_dfs.append(situacao_df)
        
        if not baixa_dfs:
            self.logger.error("❌ No baixa situacao files loaded!")
            return None
        
        # Union all baixa situacoes
        all_baixas = baixa_dfs[0]
        for df in baixa_dfs[1:]:
            all_baixas = all_baixas.unionByName(df, allowMissingColumns=True)

        self.logger.info("✅ Data already filtered by eligible processes via predicate pushdown")
        eligible_baixas = all_baixas
        
        # Deduplicate by process (keep first occurrence)
        eligible_baixas = (eligible_baixas
                          .withColumn("row_number", 
                                    row_number().over(
                                        Window.partitionBy("coddoc")
                                              .orderBy("dthrmov")
                                    ))
                          .filter(col("row_number") == 1)
                          .drop("row_number"))
        
        # Add metadata
        result_df = eligible_baixas.withColumn("variavel", lit("baixm01")) \
                                 .withColumn("meta_indicator", lit("meta_01")) \
                                 .withColumn("dth_referencia", lit(self.period_end)) \
                                 .withColumn("dth_calculo", current_timestamp()) \
                                 .withColumn("periodo_inicio", lit(self.period_start)) \
                                 .withColumn("periodo_fim", lit(self.period_end))
        
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        
        self.logger.info(f"✅ baixm01 calculation completed records in {calculation_time:.2f}s")
        
        return result_df

    def _calculate_susm01_with_eligible_processes(self) -> DataFrame:
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating susm01 (refactored logic)...")

        from pyspark.sql.functions import (
        col, lit, current_timestamp,
        row_number, year, month, trim, desc, asc
        )
        from pyspark.sql.window import Window
        from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
        )
        # Step 1: eligible_coddocs = dism minus julgadom01
        from utils.parquet_utils import meta_export_path
        meta_export_path = meta_export_path( f"meta_01", "ejud")
        dism_path = f"{meta_export_path}/cnm01.parquet"
        desm_path = f"{meta_export_path}/desm01.parquet"
        julgadom_path = f"{meta_export_path}/julgadom01.parquet"
        sit_schema = StructType([
            StructField("coddoc", LongType(), True)
        ])
        # Diagnostic: verify parquet paths are visible before attempting to read
        try:
            from utils.parquet_utils import path_exists
            self.logger.info(f"Path check julgadom: {julgadom_path} -> {path_exists(julgadom_path, self.spark)}")
        except Exception:
            self.logger.debug("Could not run parquet path diagnostic for julgadom")

        julgadom_df = self.spark.read.schema(sit_schema).parquet(julgadom_path).select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))

        susp_situacoes = self.config_manager.get_situacao_codes( f"metas", "suspenso_situacoes")
        self.logger.info(f"📂 Loading suspenso situacoes: {susp_situacoes}")

        susp_dfs = []
        for situacao in susp_situacoes:
            situacao_df = self.load_situacao_with_coddoc_filter(
                situacao,
                apply_period_filter=True,
                period_start=self.meta01_cutoff_date, 
                period_end=self.period_end
            )
            if situacao_df is not None:
                susp_dfs.append(situacao_df)
        
        if not susp_dfs:
            self.logger.error("❌ No suspenso situacao files loaded with 2026 movements!")
            return None

        # Union all suspenso situacoes
        all_suspensos = susp_dfs[0]
        for df in susp_dfs[1:]:
            all_suspensos = all_suspensos.unionByName(df, allowMissingColumns=True)


        # 6) Keep latest closed 25 per coddoc
        window_susp = Window.partitionBy("coddoc").orderBy(asc("dthrmov"))
        # Keep full rows for the latest closed 25 per coddoc
        situacao_suspenso_df = (all_suspensos
            .withColumn("_rn", row_number().over(window_susp))
            .filter((col("_rn") == 1))
            .drop("_rn")
        )
        # susm01 must be in dism or in desm
        # Check dism/desm visibility and avoid hard-failing if files are temporarily unavailable
        try:
            from utils.parquet_utils import path_exists
            self.logger.info(f"Path check dism: {dism_path} -> {path_exists(dism_path, self.spark)}")
            self.logger.info(f"Path check desm: {desm_path} -> {path_exists(desm_path, self.spark)}")
        except Exception:
            self.logger.debug("Could not run parquet path diagnostics for dism/desm")

        # Read dism (or create empty DF if missing)
        try:
            dism_df = self.spark.read.schema(sit_schema).parquet(dism_path).select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))
        except Exception:
            self.logger.warning(f"dism path not found or unreadable: {dism_path} - creating empty DataFrame")
            dism_df = self.spark.createDataFrame([], sit_schema)
            dism_df = dism_df.select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))

        # Read desm (or create empty DF if missing)
        try:
            desm_df = self.spark.read.schema(sit_schema).parquet(desm_path).select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))
        except Exception:
            self.logger.warning(f"desm path not found or unreadable: {desm_path} - creating empty DataFrame")
            desm_df = self.spark.createDataFrame([], sit_schema)
            desm_df = desm_df.select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))
        union_df = dism_df.unionByName(desm_df, allowMissingColumns=True)
        situacao_suspenso_df = situacao_suspenso_df.join(union_df, "coddoc", "inner")

        result_df = situacao_suspenso_df.join(julgadom_df, "coddoc", "left_anti")

        # 10) Add metadata columns to the full-row result DataFrame
        result_df_final = result_df.withColumn("variavel", lit(f"susm01")) \
            .withColumn("meta_indicator", lit( f"meta_01")) \
            .withColumn("dth_referencia", lit(self.period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(self.period_start)) \
            .withColumn("periodo_fim", lit(self.period_end))
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ susm calculation completed records in {calculation_time:.2f}s")
        return result_df_final
   
    def _calculate_cnm01(self) -> DataFrame:
        calculation_start = datetime.now()
        self.logger.info("🚀 Calculating EJUD cnm01 (true first-ever start in period)")

        # cnm01 rule: a process is "new" only if its FIRST-EVER start situacao
        # falls inside [meta01_cutoff_date, period_end]. Only real start codes
        # count — 61 (conhecimento) and 9/65/81 (outros). We intentionally do
        # NOT include 24 or 88: 88 (pendente) with dth_fim NULL can leak in and
        # incorrectly mark an old process as new (e.g. coddoc 11621157 whose
        # real sit-61 is 2015-12-18 must never appear in cnm01 for 2026).
        start_situacoes = [61, 9, 65, 81]
        self.logger.info(f"📂 Loading all-history start situacoes: {start_situacoes}")

        distr_df = []
        for situacao in start_situacoes:
            situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=self.eligible_processes_df,
                apply_period_filter=False,  # all history — we need the TRUE first
            )
            if situacao_df is not None:
                distr_df.append(situacao_df)
        self.logger.info(f"📂 Loaded {len(distr_df)} start-situacao DataFrames")

        if distr_df:
            result_distr_df = distr_df[0]
            for df in distr_df[1:]:
                result_distr_df = result_distr_df.unionByName(df, allowMissingColumns=True)
        else:
            result_distr_df = self.misc._create_empty_result_df()

        # Earliest start situacao per coddoc across ALL history
        window = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        result_distr_df = (result_distr_df
                           .withColumn("row_number", row_number().over(window))
                           .filter(col("row_number") == 1)
                           .drop("row_number"))

        # Keep only processes whose true first start lands inside the Meta 01 window
        result_distr_df = result_distr_df.filter(
            (col("dthrmov") >= lit(self.meta01_cutoff_date))
            & (col("dthrmov") <= lit(self.period_end))
        )

        if result_distr_df and not result_distr_df.isEmpty():
            # Add metadata and required columns
            result_distr_df = (result_distr_df
                        .withColumn("variavel", lit("cnm01"))
                        .withColumn("meta_indicator", lit("meta_01"))
                        .withColumn("dth_referencia", lit(self.period_end))
                        .withColumn("dth_calculo", current_timestamp())
                        .withColumn("periodo_inicio", lit(self.period_start))
                        .withColumn("periodo_fim", lit(self.period_end))
                        .withColumn("ano", year(col("dthrmov")))
                        .withColumn("mes", month(col("dthrmov")))
                        .withColumn("origin_data", lit("meta03")))

            calculation_time = (datetime.now() - calculation_start).total_seconds()
            self.logger.info(f"✅ cnm01 completed records in {calculation_time:.2f}s")

            return result_distr_df
        else:
            self.logger.warning("⚠️ DISM optimizer returned no results")
            return self.misc._create_empty_result_df()
                
    def _calculate_decm01(self) -> DataFrame:
        """
        Implements DECm01: Sum of occurrences of Procedimento incidental ou cautelar resolvido (140) in the process, first occurrence in the month.
        """
        logger.info("🚀 Calculating decm01 (decisions in month)")
        decm01_codes = self.config_manager.get_situacao_codes("metas", "decm_situacoes")
        df_decm01 = None
        for code in decm01_codes:
            df = self.load_situacao_with_coddoc_filter(code, apply_period_filter=True, period_start=self.meta01_cutoff_date, period_end=self.period_end)
            if df is not None:
                if df_decm01 is None:
                    df_decm01 = df
                else:
                    df_decm01 = df_decm01.unionByName(df, allowMissingColumns=True)
        if df_decm01 is None:
            logger.warning("No decm01 data found")
            return self.misc._create_empty_result_df()
        # Only first occurrence per process
        window = Window.partitionBy("coddoc").orderBy("dthrmov")
        df_decm01 = df_decm01.withColumn("row_number", row_number().over(window)).filter(col("row_number") == 1).drop("row_number")
        df_decm01 = df_decm01.withColumn("variavel", lit("decm01")) 
                          
        return self._select_and_order_meta_columns(df_decm01)

    def _calculate_desm01_with_eligible_processes(self) -> DataFrame:
        from pyspark.sql.functions import col, lit, year, month, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        from utils.parquet_utils import meta_export_path
        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating desm01 (unsuspended processes for Meta)...")
        # Step 0: Load 25 using all eligible processes by class filtering by meta period
        _meta01_export_path = meta_export_path( f"meta_01", "ejud")
    
        situacao_25 = self.load_situacao_with_coddoc_filter(
            situacao=25,
            apply_period_filter=True,
            period_start=self.meta01_cutoff_date,
            period_end=self.period_end
        )        
        
        # Keep earliest opened 25 in the month per coddoc (first unsuspension).
        window_first = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        first_25 = (situacao_25.withColumn("row_num", row_number().over(window_first))
                        .filter((col("row_num") == 1))
                        .drop("row_num"))
        first_opened_25 = first_25.filter((col("dth_fim").isNull()))
        
        if first_opened_25 is None:
            return self.misc._create_empty_result_df()
        
        # Step 1: Remove 25 that are in julgadom
        julgadom_path = f"{_meta01_export_path}/julgadom01.parquet"

        julgadom_df = self.spark.read.parquet(julgadom_path).select("coddoc")
        opened_25_clean = first_opened_25.join(julgadom_df, "coddoc", "left_anti")
        self.logger.info(f"✅ Removed julgados with success")
        first_25_coddoc = opened_25_clean.select("coddoc")                        
        
        # Step 2: Confirm that the processes are pending in the start of the period
        closed_25 = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=25,
            coddoc_list=first_25_coddoc,
            apply_period_filter=True,
            period_start=date(1988,1,1),
            period_end=self.meta01_cutoff_date
        )        
        
        window_first = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        last_25 = (closed_25.withColumn("row_num", row_number().over(window_first))
                        .filter((col("row_num") == 1))
                        .drop("row_num"))
        last_25 = last_25.filter((col("dth_fim").isNotNull())).select("coddoc")

        opened_88 = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=88,
            coddoc_list=first_25_coddoc,
            apply_period_filter=True,
            period_start=date(1988,1,1),
            period_end=self.meta01_cutoff_date
        )        
        
        window_first = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        last_88 = (opened_88.withColumn("row_num", row_number().over(window_first))
                        .filter((col("row_num") == 1))
                        .drop("row_num"))
        last_88 = last_88.filter((col("dth_fim").isNull())).select("coddoc")
        union_pending = last_25.join(last_88, "coddoc", "inner")
        
        # Check if the open 25 was pending
        final_df = opened_25_clean.join(union_pending, "coddoc", "inner")
        # Add metadata columns and return
        result_df = (final_df
                    .withColumn("variavel", lit(f"desm01"))
                    .withColumn("meta_indicator", lit( f"meta_01"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end))
                    )

        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ desm01 calculation completed records in {calculation_time:.2f}s")

        return result_df


    def _configure_meta01_memory_optimizations(self):
        """Configure Spark for Meta 01 memory optimization with advanced features - ENHANCED WITH CODEC FALLBACK."""
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
            spark_conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
            
            # CRITICAL FIX: Compression codec with Snappy fallback handling
            #self._configure_compression_codec_with_fallback(spark_conf)
            
            
            self.logger.info("✅ Applied Meta 01 enhanced memory and performance optimizations with codec fallback")
            
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

    def _create_empty_result_df(self) -> DataFrame:
        """Create an empty DataFrame with the standard result schema for Meta 02."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
        
        empty_schema = StructType([
            StructField("coddoc", LongType(), True),
            StructField("dthrmov", TimestampType(), True),
            StructField("dth_fim", TimestampType(), True),
            StructField("situacao", IntegerType(), True),
            StructField("variavel", StringType(), True)
        ])
        return self.spark.createDataFrame([], empty_schema)
    
    def _create_checkpoint(self, df, name="checkpoint"):
        """
        Checkpoint a Spark DataFrame and return the checkpointed DataFrame.
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

    def _validate_active_processes(self, df: DataFrame, variable_name: str) -> DataFrame:
        from pyspark.sql.functions import (
            col, lit, current_timestamp,
            row_number, year, month
        )
        try:
            self.logger.info(f"🔍 Validating active processes for {variable_name}...")
            
            # Extract unique coddoc values from the result set
            coddoc_to_validate = df.select("coddoc").distinct()
            self.logger.info(f"📊 Unique processes to validate:")
            
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
                
            except AttributeError:
                # Fallback: hybrid_loader doesn't have load_documento_with_coddoc_filter method
                self.logger.warning("⚠️ Hybrid loader documento method not available - using fallback approach")
                return None
                
            except Exception as load_error:
                self.logger.warning(f"⚠️ Error using hybrid loader for documento validation: {load_error}")
                self.logger.info("🔄 Falling back to optimized approach...")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error in active process validation: {e}")
            self.logger.warning("⚠️ Skipping active process validation - returning all processes")
            return None

    def _load_documento_for_coddocs(self, coddoc_list: DataFrame, documento_base_path: str) -> Optional[DataFrame]:
        try:
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
                # This is much more efficient than loading all documento data
                filtered_sql = """
                SELECT d.* 
                FROM documento_all d
                INNER JOIN validation_coddocs v ON d.coddoc = v.coddoc
                """
                
                documento_filtered = self.spark.sql(filtered_sql)
                
                self.logger.info(f"📊 Loaded documento records for validation")
                
                return documento_filtered

            except Exception as load_error:
                self.logger.error(f"❌ Error loading documento data: {load_error}")
                return None  
            
                
        except Exception as e:
            self.logger.error(f"❌ Error loading documento for coddocs: {e}")
            return None

  
    def calculate_all_meta01_spark(self) -> bool:
        """
        Calculate all Meta 01 variables using Spark distributed processing.
        
        Returns:
            bool: True if all calculations completed successfully
        """
        self.logger.info("🚀 Starting comprehensive Meta 01 calculation with Spark...")
        
        start_time = datetime.now()
        results = {}
        
        # Define calculation methods for all Meta 01 variables
        calculations = [
            ("cnm01", self._calculate_cnm01),
            ("baixm01", self._calculate_baixm01_with_eligible_processes),
            ("primeirasentm01", self._calculate_primeirasentm01_with_eligible_processes),
            ("mpum01", self._calculate_mpum01_with_eligible_processes),
            ("decm01", self._calculate_decm01),
            ("julgadom01", self._calculate_julgadom01_with_eligible_processes),
            ("susm01", self._calculate_susm01_with_eligible_processes),
            ("desm01", self._calculate_desm01_with_eligible_processes)
        ]

        for variable_name, calculation_method in calculations:
            try:
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"🎯 Calculating {variable_name.upper()}")
                self.logger.info(f"{'='*60}")
                
                calculation_start = datetime.now()
                result = calculation_method()
                
                if result is not None:
                    # Enforce output schema
                    result = self._select_and_order_meta_columns(result)

                    # If incremental mode is enabled, merge with existing exported variable file
                    if getattr(self, 'incremental', False):
                        try:
                            from utils.parquet_utils import meta_export_path
                            import os
                            from pyspark.sql.functions import row_number, col
                            from pyspark.sql.window import Window

                            meta_path = meta_export_path(f"meta_01", "ejud")
                            var_path = f"{meta_path}/{variable_name}.parquet"
                            if os.path.exists(var_path):
                                self.logger.info(f"🔁 Incremental mode: existing export found for {variable_name}, merging results")
                                existing_df = self.spark.read.option("recursiveFileLookup", "true").parquet(var_path)
                                # Union existing with new results (allow missing columns)
                                combined = existing_df.unionByName(result, allowMissingColumns=True)

                                # Deduplicate by coddoc keeping the most recent record by dth_calculo then dthrmov
                                w = Window.partitionBy("coddoc").orderBy(col("dth_calculo").desc(), col("dthrmov").desc())
                                combined = combined.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
                                result = combined
                                self.logger.info(f"🔀 Merged result for {variable_name} records after dedupe")
                            else:
                                self.logger.info(f"🔁 Incremental mode: no existing export for {variable_name}, writing new results")
                        except Exception as merge_err:
                            self.logger.warning(f"⚠️ Incremental merge failed for {variable_name}: {merge_err} - falling back to full write")
                            # keep result as-is
                            pass

                    # Create final checkpoint before export
                    #result = self._create_checkpoint(result, f"{variable_name}_final")
                    
                    incremental_mode = self.incremental_months > 0
                    if self.results_exporter.export_results(result, variable_name, incremental_mode=incremental_mode):
                        self.logger.info(f"✅ {variable_name} exported records")
                    else:
                        self.logger.error(f"❌ Failed to export {variable_name}")
                    # Release DataFrame and clean up memory for next calculation
                    del result
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
        
        
        self.calculation_results = results
        if self.calculation_results is not None:
            return True
        else:
            self.logger.warning(f"⚠️ calculations completed")
            return False

    def load_situacao_with_coddoc_filter(self, situacao: int, 
                                        apply_period_filter: bool = False,
                                        period_start: Optional[date] = None,
                                        period_end: Optional[date] = None) -> DataFrame:
        """
        Args:
            situacao: Situacao code to load
            apply_period_filter: Whether to apply period filtering
            period_start: Start date for period filtering
            period_end: End date for period filtering
            
        Returns:
            Spark DataFrame with filtered situacao data (only eligible processes)
        """
        from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
    )
        empty_schema = StructType([
            StructField("coddoc", LongType(), True),
            StructField("dthrmov", TimestampType(), True),
            StructField("dth_fim", TimestampType(), True),
            StructField("situacao", IntegerType(), True),
            StructField("variavel", StringType(), True)
        ])

        if situacao not in self.available_situacoes:
            self.logger.warning(f"Situacao {situacao} not available - returning empty DataFrame")
            return self.spark.createDataFrame([], empty_schema)

        if self.eligible_processes_df is None:
            self.logger.warning(f"No eligible processes available (DataFrame is None) - returning empty DataFrame")
            return self.spark.createDataFrame([], empty_schema)

        # Check if eligible processes DataFrame is empty
        try:
            eligible_processes_list = self.eligible_processes_df.select("coddoc").distinct()
            self.logger.info(f"📊 Sending eligible processes to hybrid loader for situacao {situacao}")
        except Exception as e:
            self.logger.error(f"Error checking eligible processes: {e}")
            return self.spark.createDataFrame([], empty_schema)

        try:
            self.logger.info(f"🔄 Calling hybrid_loader.load_situacao_with_coddoc_filter for situacao {situacao}")
            self.logger.info(f"   📂 situacao_base_path: {self.situacao_base_path}")
            self.logger.info(f"   📅 apply_period_filter: {apply_period_filter}")
            self.logger.info(f"   📅 period_start: {period_start}")
            self.logger.info(f"   📅 period_end: {period_end}")

            eligible_processes_list_long = eligible_processes_list.withColumn("coddoc", col("coddoc").cast(LongType()))

            df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=eligible_processes_list_long,
                situacao_base_path=self.situacao_base_path,
                date_column="dthrmov",
                apply_period_filter=apply_period_filter,
                period_start=period_start,
                period_end=period_end
            )
            if df is None:
                self.logger.error(f"❌ Hybrid loader returned None for situacao {situacao}")
                return self.spark.createDataFrame([], empty_schema)
            
            if df is not None:
                self.logger.info(f"✅ Successfully loaded situacao {situacao} with predicate pushdown  records")
                return df
            else:
                self.logger.warning(f"⚠️ Predicate pushdown returned no data for situacao {situacao}")
                return self.spark.createDataFrame([], empty_schema)
        except Exception as e:
            self.logger.error(f"❌ Failed to load situacao {situacao} with predicate pushdown: {e}")
            return self.spark.createDataFrame([], empty_schema)
 
    def run_meta(
        spark=None,
        config_manager=None,
        logger=None,
        misc=None,
        hybrid_loader=None,
        results_exporter=None,
        period_start: Optional[date] = None,
        period_end: Optional[date] = None,
        incremental_months: int = 0,
    ) -> bool:
        """
        Standard in-process entrypoint used by meta_cnj2026 orchestrator.
        Returns True on success, False on failure.
        """
        try:
            # Build calculator using injected/shared resources (or local creation)
            calc = MetaCNJ2025CalculatorEJUD_Meta01_Production(
                app_name="MetaCNJ2025_Meta01_Spark",
                use_recovery=True,
                period_start=period_start,
                period_end=period_end,
                spark=spark,
                config_manager=config_manager,
                logger=logger,
                misc=misc,
                hybrid_loader=hybrid_loader,
                results_exporter=results_exporter,
                incremental_months=incremental_months,
            )

            # Run the usual orchestrated calculation entry
            success = calc.calculate_all_meta01_spark()
            return bool(success)
        except Exception as e:
            # Use provided logger if available, else create a local one
            safe_logger = logger if logger is not None else logging.getLogger("meta_01_ejud")
            safe_logger.exception(f"💥 run_meta failed: {e}")
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
    """Main execution function for production Meta 01 calculations."""
    parser = argparse.ArgumentParser(description='Meta 01 EJUD Production Calculator')
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
        # Create a dedicated SparkSession for this script
        spark = (
            SparkMetaUtils.get_optimized_spark_config("Meta_01")
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

        # Initialize calculator with dedicated SparkSession
        calculator = MetaCNJ2025CalculatorEJUD_Meta01_Production(
            period_start=period_start,
            period_end=period_end,
            incremental_months=args.incremental,
            spark=spark
        )
        
        # Run calculations
        success = calculator.calculate_all_meta01_spark()
        
        if success:
            logger.info("🎉 Meta 01 production calculations completed successfully!")
            sys.exit(0)
            
        else:
            logger.error("❌ Meta 01 calculations failed")
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
