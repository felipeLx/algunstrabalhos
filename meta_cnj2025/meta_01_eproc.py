from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import argparse
import sys
import os
import logging
import glob
import gc
import traceback
import tempfile
import shutil

# PySpark imports - consolidated at top level
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, year, desc, asc, current_timestamp, row_number, 
    regexp_replace, trim, when, to_date, month, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType
)
from pyspark import StorageLevel

# Add project root to Python path for utils imports
sys.path.insert(0, str(Path(__file__).parent))

# Import enhanced utilities from utils module
from utils.hybrid_parquet_loader import HybridParquetLoader
from utils.spark_meta_utils import SparkMetaMixin, SparkMetaUtils
from utils.meta_results_exporter import MetaResultsExporter
from utils.meta_logging_system import MetaLogger
from utils.meta_config_manager import MetaConfigManager
from utils.misc import MetaMiscellaneous, safe_stop_spark
from utils.parquet_utils import parquet_path, meta_export_path
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

class MetaCNJ2025CalculatorEPROC_Meta01_Production(SparkMetaMixin):
    
    def __init__(self, app_name: str = "MetaCNJ2025_Meta01_Spark", use_recovery: bool = True, 
                period_start: Optional[date] = None, period_end: Optional[date] = None,
                spark: "SparkSession" = None):
        """Initialize Spark calculator with optimized configuration for Meta 01 EPROC."""
        
        # Initialize configuration manager
        self.config_manager = MetaConfigManager()

        # Debug configuration loading
        logger.info(f"🔍 Config manager initialized")
        logger.info(f"� Current working directory: {os.getcwd()}")
        logger.info(f"�📁 Config directory: {self.config_manager.config_dir}")
        logger.info(f"📋 Meta config keys: {list(self.config_manager.meta_config.keys())}")
        
        # Test meta_01 config specifically
        meta01_config = self.config_manager.get_meta_config("meta_01")
        if meta01_config:
            logger.info(f"✅ Meta 01 config found with keys: {list(meta01_config.keys())}")
        else:
            logger.error(f"❌ No meta_01 configuration found!")
        
        # Initialize enhanced logging
        self.meta_logger = MetaLogger("01")
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
            
        # Initialize results exporter
        self.results_exporter = MetaResultsExporter(self.spark, "01", config_manager=self.config_manager)
        # Load paths from configuration with fallbacks
        self.eproc_base_path = self.config_manager.get_system_path("eproc_base_path") or "/opt/parquet/data/prata/eproc2g"
        self.situacao_base_path = self.config_manager.get_system_path("situacao_eproc_base_path") or "/opt/parquet/data/prata/eproc2g/situacao_eproc"
        self.use_recovery = use_recovery
        
        # Log the paths being used
        if not self.config_manager.get_system_path("eproc_base_path"):
            self.logger.warning(f"⚠️ eproc_base_path not configured, using fallback: {self.eproc_base_path}")
        else:
            self.logger.info(f"📁 EPROC base path: {self.eproc_base_path}")
            
        if not self.config_manager.get_system_path("situacao_eproc_base_path"):
            self.logger.warning(f"⚠️ situacao_eproc_base_path not configured, using fallback: {self.situacao_base_path}")
        else:
            self.logger.info(f"📁 Situacao base path: {self.situacao_base_path}")
        
        # Load date configuration from JSON with fallback defaults
        self.period_end = self.config_manager.get_date_cutoff("meta_01", "period_end") 
        self.period_start = self.config_manager.get_date_cutoff("meta_01", "period_start") 
        self.meta01_cutoff_date = self.config_manager.get_date_cutoff("meta_01", "meta01_cutoff_date") 
        self.meta01_15year_cutoff = (self.config_manager.get_date_cutoff("meta_01", "meta01_15year_cutoff") or 
                                   date(self.period_end.year - 15, 1, 1))
        
        # Initialize hybrid loader for problematic parquet files (disable Meta 03 filtering for Meta 01)
        # IMPORTANT: This must be initialized BEFORE loading eligible processes!
        # self = MetaMiscellaneous(spark = self.spark)
        self.misc = MetaMiscellaneous(
            spark = self.spark,
            logger = self.logger,
            meta_number = "01"
        )
        
        # Initialize eligible processes after hybrid loader is ready
        self.eligible_processes_df = self._load_meta01_eligible_processes()
        
        # Load available situacoes for validation (EPROC-specific)
        self.available_situacoes = self.config_manager.get_available_situacoes("metas")
        
        # Apply Meta 01 specific memory optimizations
        self._configure_meta01_memory_optimizations()
        
        self.logger.info(f"📅 Meta 01 EPROC date configuration:")
        self.logger.info(f"   📊 Period: {self.period_start} to {self.period_end}")
        self.logger.info(f"   � meta01_cutoff_date: {self.meta01_cutoff_date}")
        self.logger.info(f"   �📂 EPROC base path: {self.eproc_base_path}")
        self.logger.info(f"   📂 Situacao base path: {self.situacao_base_path}")
        self.logger.info(f"   📋 Available situacoes: {len(self.available_situacoes)} codes")
        
        # Store results for summary
        self.calculation_results = {}
        # helpers for safer cache/unpersist (mirror EPROC2G implementation)
        def _safe_unpersist(df):
            try:
                if df is None:
                    return
                df.unpersist(blocking=False)
            except Exception:
                try:
                    df.unpersist()
                except Exception:
                    pass
        self._safe_unpersist = _safe_unpersist

        def _safe_cache(df, partitions=None):
            try:
                if partitions is not None:
                    df = df.repartition(partitions)
                return df.persist(StorageLevel.MEMORY_AND_DISK)
            except Exception:
                try:
                    return df.cache()
                except Exception:
                    return df
        self._safe_cache = _safe_cache

    def _load_meta01_eligible_processes(self) -> DataFrame:
        # Types already imported at top level
        
        self.logger.info("🔄 Loading Meta 01 EPROC eligible processes using CONHECIMENTO filter...")
        
        try:
            # Step 1: Load CONHECIMENTO codes from classes.xlsx
            conhecimento_codes = self._load_conhecimento_codes_from_excel()
            if not conhecimento_codes:
                self.logger.error("❌ Failed to load CONHECIMENTO codes from classes.xlsx")
                # Return empty DataFrame instead of None
                return self.spark.createDataFrame([], StructType([StructField("id_processo", StringType(), True)]))
            
            self.logger.info(f"📋 Found {len(conhecimento_codes)} CONHECIMENTO procedure codes")
            
            # Step 2: Load EPROC class hierarchy tables from bronze layer
            eproc_bronze = f"/opt/parquet/data/bronze/eproc2g"
            
            # Load mni_classe_judicial (contains cod_mni_classe_judicial)
            mni_classe_path = f"{eproc_bronze}/mni_classe_judicial"
            self.logger.info(f"📂 Loading from: {mni_classe_path}")
            
            # Validate path exists before trying to read
            if not os.path.exists(mni_classe_path):
                self.logger.error(f"❌ Path does not exist: {mni_classe_path}")
                self.logger.error(f"💡 Please check if:")
                self.logger.error(f"   1. The EPROC bronze data is available at: {eproc_bronze}")
                self.logger.error(f"   2. The mni_classe_judicial parquet files exist")
                self.logger.error(f"   3. The config file has correct eproc_bronze_path setting")
                raise FileNotFoundError(f"EPROC bronze path not found: {mni_classe_path}")
            
            # Configure Spark for timestamp_ntz/INT96 compatibility before reading bronze tables
            self.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
            self.spark.conf.set("spark.sql.parquet.int96AsTimestamp", "true")
            self.spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
            self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
            
            mni_classe_df = self.spark.read.parquet(mni_classe_path)
            
            # Unique cod_mni_classe_judicial values
            unique_mni_codes = mni_classe_df.select("cod_mni_classe_judicial").distinct().collect()
            unique_mni_list = [row.cod_mni_classe_judicial for row in unique_mni_codes]
            
            # Check intersection between conhecimento_codes and MNI codes
            conhecimento_set = set(conhecimento_codes)
            mni_set = set(unique_mni_list)
            intersection = conhecimento_set.intersection(mni_set)
            
            self.logger.info(f"🔍 Intersection analysis:")
            self.logger.info(f"   📊 CONHECIMENTO codes: {len(conhecimento_codes)}")
            self.logger.info(f"   📊 MNI codes in DB: {len(unique_mni_list)}")
            self.logger.info(f"   ✅ Matching codes: {len(intersection)}")
            self.logger.info(f"   📋 Matching codes: {sorted(list(intersection))}")
            
            # Codes that don't match
            conhecimento_not_in_mni = conhecimento_set - mni_set
            mni_not_in_conhecimento = mni_set - conhecimento_set
            
            if conhecimento_not_in_mni:
                self.logger.info(f"   ⚠️ CONHECIMENTO codes not in MNI: {len(conhecimento_not_in_mni)}")
                self.logger.info(f"   📋 Missing from MNI: {sorted(list(conhecimento_not_in_mni))[:10]}...")
                
            if mni_not_in_conhecimento:
                self.logger.info(f"   📊 MNI codes not in CONHECIMENTO: {len(mni_not_in_conhecimento)}")
                self.logger.info(f"   📋 Extra in MNI: {sorted(list(mni_not_in_conhecimento))[:10]}...")
            
            # Load relacao_classe_judicial_mni (maps id_classe_judicial to cod_mni_classe_judicial)
            relacao_classe_path = f"{eproc_bronze}/relacao_classe_judicial_mni"
            self.logger.info(f"📂 Loading from: {relacao_classe_path}")
            relacao_classe_df = self.spark.read.parquet(relacao_classe_path)
            self.logger.info('classe mni table')
            processo_path = f"{eproc_bronze}/processo"
            self.logger.info(f"📂 Loading from: {processo_path}")
            processo_df = self.spark.read.parquet(processo_path)
            self.logger.info('processo table')
            
            # Step 3: Filter mni_classe_judicial by CONHECIMENTO codes
            self.logger.info("🔍 Step 3: Filtering MNI classes by CONHECIMENTO codes...")
            filtered_mni = mni_classe_df.filter(
                col("cod_mni_classe_judicial").isin(conhecimento_codes)
            )
            
            # Step 4: Join hierarchy to get eligible id_classe_judicial values
            # mni_classe_judicial -> relacao_classe_judicial_mni
            self.logger.info("🔗 Step 4: Joining with relacao_classe_judicial_mni...")
            eligible_classes = filtered_mni.join(
                relacao_classe_df,
                filtered_mni.cod_mni_classe_judicial == relacao_classe_df.cod_mni_classe_judicial,
                "inner"
            ).select("id_classe_judicial").distinct()
            
            eligible_processes = processo_df.join(
                eligible_classes,
                "id_classe_judicial",
                "inner"
            ).select("id_processo").distinct()
            
            # Convert id_processo to StringType for BigInt compatibility
            eligible_processes = eligible_processes.withColumn("id_processo", col("id_processo").cast(StringType()))
            
            return eligible_processes
            
        except Exception as e:
            self.logger.error(f"❌ Error loading Meta 01 EPROC eligible processes: {e}")
            import traceback
            self.logger.error(f"📋 Traceback: {traceback.format_exc()}")
            # Return empty DataFrame instead of None
            return self.spark.createDataFrame([], StructType([StructField("id_processo", StringType(), True)]))
    
    def _load_conhecimento_codes_from_excel(self) -> List[str]:
        """
        Load CONHECIMENTO procedure codes from classes.xlsx file.
        
        Returns:
            List of codes where "Tipo de Procedimento" equals "CONHECIMENTO"
        """
        try:
            import pandas as pd
            
            excel_path = "csv/classes.xlsx"
            self.logger.info(f"📖 Reading classes.xlsx from {excel_path}")
            
            # Read Excel file
            df = pd.read_excel(excel_path)
            self.logger.info(f"📊 Excel file loaded: {len(df)} rows, columns: {list(df.columns)}")
            
            conhecimento_df = df[df.iloc[:, 2] == "CONHECIMENTO"]
            self.logger.info(f"📊 CONHECIMENTO records found: {len(conhecimento_df)}")
            
            if len(conhecimento_df) == 0:
                self.logger.warning("⚠️ No CONHECIMENTO records found! Checking for similar terms...")
                # Try case-insensitive or partial matches
                conhecimento_like = df[df.iloc[:, 2].astype(str).str.upper().str.contains("CONHECIMENTO", na=False)]
                self.logger.info(f"📊 Case-insensitive CONHECIMENTO matches: {len(conhecimento_like)}")
                if len(conhecimento_like) > 0:
                    conhecimento_df = conhecimento_like
            
            # Extract codes from first column
            conhecimento_codes = conhecimento_df.iloc[:, 0].astype(str).tolist()
            
            return conhecimento_codes
            
        except Exception as e:
            self.logger.error(f"❌ Error reading classes.xlsx: {e}")
            import traceback
            self.logger.error(f"📋 Traceback: {traceback.format_exc()}")
            return []
   
    def _calculate_mpum01_with_eligible_processes(self) -> DataFrame:
        """
        Implements mpum01: Soma de ocorrências das situações Decisão proferida (17) e Medida protetiva homologada ou revogada (34),
        restrita aos movimentos 11423, 11424, 11425, 12476, 12479, 15486, 15487 e 15488, primeira ocorrência no processo no mês analisado.
        """
        calculation_start = datetime.now()
        self.logger.info("🚀 Calculating mpum01 (medidas protetivas e decisões proferidas no mês para Meta 01)...")
        situacao_codes = [17, 34]
        
        mpum_dfs = []
        for situacao in situacao_codes:
            situacao_df = self.load_situacao_with_id_processo_filter(
                situacao,
                apply_period_filter=True,
                period_start=self.meta01_cutoff_date,  # cutoff for all others
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

        # Data is already filtered by eligible processes (id_processo predicate pushdown)
        # No need for expensive join - data only contains eligible processes
        self.logger.info("✅ Data already filtered by eligible processes via predicate pushdown")
        eligible_mpums = all_mpums.filter(col("cod_mni_complemento").isin(self.config_manager.get_situacao_codes("metas", "valid_movement_codes")))
        eligible_mpums = (eligible_mpums
                         .withColumn("row_number", 
                                    row_number().over(
                                        Window.partitionBy("id_processo")
                                              .orderBy("dth_inicio")
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
        
        # Optimize and cache
        result_df = result_df.repartition(400, "situacao").cache()
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ mpum01 calculation completed records in {calculation_time:.2f}s")

        return result_df
    
    def _calculate_julgadom01_with_eligible_processes(self) -> DataFrame:
        """
        Calculate julgadom01 (judged processes) using Meta 01 eligible processes.
        """
        calculation_start = datetime.now()
        self.logger.info("🚀 Calculating julgadom01...")
        
        # Collect results from constituent variables
        constituent_results = []
        # Use the same EPROC export path structure as in _export_eproc_results_locally
        eproc_export_path = "/opt/parquet/data/prata/eproc2g/meta_cnj2026/meta_01/current"
        
        # Load constituent variables from parquet
        for variable in ["primeirasentm01", "baixm01", "mpum01"]:
            parquet_path = f"{eproc_export_path}/{variable}.parquet"
            self.logger.info(f"📂 Looking for: {parquet_path}")
            
            try:
                # Check if file exists before trying to read
                import os
                if os.path.exists(parquet_path):
                    df = self.spark.read.option("recursiveFileLookup", True).parquet(parquet_path)
                    if df is not None:
                        constituent_results.append(df)
                        self.logger.info(f"✅ Loaded {variable}")
                    else:
                        self.logger.warning(f"⚠️ {variable} file exists but is empty")
                else:
                    self.logger.warning(f"⚠️ {variable} file not found: {parquet_path}")
            except Exception as e:
                self.logger.error(f"❌ Error reading {variable}: {e}")
        
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
                                         Window.partitionBy("id_processo")
                                               .orderBy("dth_inicio")
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
                    .withColumn("periodo_fim", lit(self.period_end))
                    .withColumn("ano", year(col("dth_inicio")))
                    .withColumn("mes", month(col("dth_inicio")))
                    .withColumn("origin_data", lit("violencia_domestica_feminicidio")))
        
        result_df = result_df.repartition(400, "id_processo").cache()
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ julgadom01 completed records in {calculation_time:.2f}s")
        
        return result_df

    def _calculate_primeirasentm01_with_eligible_processes(self) -> DataFrame:
        """
        Calculate primeirasentm01 (first sentences) using Meta 01 eligible processes.
        Ensures intersection filter and date cut are applied.
        """
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating primeirasentm01 (first sentences for Meta 01)...")
        
        # Load sentencing situacao codes from configuration
        sentencing_situacoes = self.config_manager.get_situacao_codes("metas", "prim_sent_situacoes")
        self.logger.info(f"📂 Loading sentencing situacoes: {sentencing_situacoes}")
        
        sentencing_dfs = []
        for situacao in sentencing_situacoes:
            situacao_df = self.load_situacao_with_id_processo_filter(
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
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
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
        result_df = result_df.repartition(400, "situacao").cache()
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ primeirasentm01 calculation completed records in {calculation_time:.2f}s")
        
        return result_df

    def _calculate_baixm01_with_eligible_processes(self) -> DataFrame:
        """
        Calculate baixm01 (archived processes) using Meta 01 eligible processes.
        Ensures intersection filter and date cut are applied.
        """
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating baixm01 (archived processes for Meta 01)...")
        
        # Load baixa situacao codes from configuration
        baixa_situacoes = self.config_manager.get_situacao_codes("metas", "baixa_situacoes")
        self.logger.info(f"📂 Loading baixa situacoes: {baixa_situacoes}")
        
        baixa_dfs = []
        for situacao in baixa_situacoes:
            situacao_df = self.load_situacao_with_id_processo_filter(
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
                                        Window.partitionBy("id_processo")
                                              .orderBy("dth_inicio")
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
        
        # Optimize and cache
        result_df = result_df.repartition(400, "situacao").cache()
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ baixm01 calculation completed records in {calculation_time:.2f}s")
        
        return result_df

    def _calculate_susm01_with_eligible_processes(self) -> DataFrame:
        from datetime import datetime
        import glob
        import os
        
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating susm01 (NEW direct parquet approach)...")

        # Step 1: Load julgadom01 for later exclusion
        from utils.parquet_utils import meta_export_path
        meta_export_path = meta_export_path( f"meta_01", "eproc2g")
        julgadom_path = f"{meta_export_path}/julgadom01.parquet"
        sit_schema = StructType([
            StructField("id_processo", StringType(), True)
        ])
        julgadom_df = self.spark.read.schema(sit_schema).parquet(julgadom_path).select("id_processo").distinct()

        # Step 2: Define suspenso situacoes
        susp_situacoes = [128,135,92,93,46,45,94,95,47,144,96,48,49]
        self.logger.info(f"📂 Loading suspenso situacoes: {susp_situacoes}")
        self.logger.info(f"🔍 DATE CONFIGURATION:")
        self.logger.info(f"   📅 meta01_cutoff_date (period_start): {self.meta01_cutoff_date}")
        self.logger.info(f"   📅 period_end: {self.period_end}")
        
        # Step 3: Read parquet files directly with recursive lookup
        all_suspenso_dfs = []
        
        for situacao in susp_situacoes:
            self.logger.info(f"🔄 Processing situacao {situacao}...")
            
            # Build parquet path pattern for old layout (partitioned by situacao)
            situacao_dir = f"{self.situacao_base_path}/situacao_partition={situacao:03d}"
            self.logger.info(f"📂 Looking in: {situacao_dir}")

            if os.path.exists(situacao_dir):
                # Use recursive file pattern to find all parquet files
                file_pattern = f"{situacao_dir}/**/part-*.parquet"
                parquet_files = glob.glob(file_pattern, recursive=True)

                self.logger.info(f"📄 Found {len(parquet_files)} parquet files for situacao {situacao}")

                if parquet_files:
                    for i, pfile in enumerate(parquet_files[:3]):
                        self.logger.info(f"   📄 File {i+1}: {pfile}")

                    try:
                        # Read all parquet files for this situacao using recursive lookup
                        situacao_df = self.spark.read.option("recursiveFileLookup", "true").parquet(situacao_dir)
                        self.logger.info(f"✅ Situacao {situacao}")

                        # Add situacao column for identification (if missing)
                        if "situacao" not in situacao_df.columns:
                            situacao_df = situacao_df.withColumn("situacao", lit(situacao))

                        all_suspenso_dfs.append(situacao_df)
                    except Exception as e:
                        self.logger.error(f"❌ Error reading situacao {situacao}: {e}")
                        continue
                else:
                    self.logger.warning(f"⚠️ No parquet files found for situacao {situacao} in {situacao_dir}")
            else:
                # Fallback: new layout where files are partitioned by year_month only
                self.logger.info(f"ℹ️ Old situacao partition dir not found. Falling back to year_month lookup under {self.situacao_base_path}")

                # Build list of year_month strings covering configured period
                months = []
                try:
                    start = self.period_start or (self.period_end.replace(day=1) if self.period_end else None)
                    end = self.period_end or start
                    if start is None or end is None:
                        self.logger.warning("⚠️ period_start/period_end not available for fallback lookup; skipping situacao")
                        continue

                    cur = start.replace(day=1)
                    while cur <= end:
                        months.append((cur.year, cur.month))
                        # increment month
                        if cur.month == 12:
                            cur = cur.replace(year=cur.year+1, month=1)
                        else:
                            cur = cur.replace(month=cur.month+1)
                except Exception as e:
                    self.logger.warning(f"⚠️ Could not build month list for fallback lookup: {e}")
                    continue

                fallback_dfs = []
                for (y, m) in months:
                    ym_dir = f"{self.situacao_base_path}/year_month={y}-{m:02d}"
                    if not os.path.exists(ym_dir):
                        self.logger.debug(f"ℹ️ No dir for year_month: {ym_dir}")
                        continue

                    try:
                        df_month = self.spark.read.option("recursiveFileLookup", "true").parquet(ym_dir)
                        # Filter rows for this situacao value
                        if "situacao" in df_month.columns:
                            df_filt = df_month.filter(col("situacao") == situacao)
                        else:
                            # If situacao column missing, try to infer or skip
                            self.logger.debug(f"ℹ️ 'situacao' column missing in {ym_dir}; skipping month for situacao {situacao}")
                            continue

                        self.logger.info(f"✅ Found records for situacao {situacao} in {y}-{m:02d}")
                        # ensure situacao column exists and has the value
                        df_filt = df_filt.withColumn("situacao", lit(situacao))
                        fallback_dfs.append(df_filt)
                    except Exception as e:
                        self.logger.warning(f"⚠️ Could not read/filter month {ym_dir}: {e}")
                        continue

                if fallback_dfs:
                    # union all months for this situacao
                    situacao_df = fallback_dfs[0]
                    for d in fallback_dfs[1:]:
                        situacao_df = situacao_df.unionByName(d, allowMissingColumns=True)

                    all_suspenso_dfs.append(situacao_df)
                else:
                    self.logger.warning(f"⚠️ No data found for situacao {situacao} in fallback year_month layout")

        if not all_suspenso_dfs:
            self.logger.error("❌ No suspenso situacao files loaded!")
            return None

        # Step 4: Union all suspenso situacoes
        self.logger.info(f"🔗 Unioning {len(all_suspenso_dfs)} situacao DataFrames...")
        all_suspensos = all_suspenso_dfs[0]
        for df in all_suspenso_dfs[1:]:
            all_suspensos = all_suspensos.unionByName(df, allowMissingColumns=True)

        # Step 5: Apply date filter
        self.logger.info(f"📅 Applying date filter...")
        
        # Handle None dates with fallbacks
        if self.meta01_cutoff_date is None:
            self.logger.warning("⚠️ meta01_cutoff_date is None, using default start date")
            start_dt = datetime(2026, 1, 1)
        else:
            start_dt = datetime.combine(self.meta01_cutoff_date, datetime.min.time())
            
        if self.period_end is None:
            self.logger.warning("⚠️ period_end is None, using current date")
            end_dt = datetime.now()
        else:
            end_dt = datetime.combine(self.period_end, datetime.min.time())
            
        self.logger.info(f"   📅 Filter range: {start_dt} to {end_dt}")
        
        # Show sample dates before filtering
        try:
            sample_dates = all_suspensos.select("dth_inicio", "situacao").limit(10).collect()
            self.logger.info("🔍 Sample dth_inicio values before filtering:")
            for i, row in enumerate(sample_dates):
                self.logger.info(f"   {i+1}: {row['dth_inicio']} (situacao {row['situacao']})")
        except Exception as e:
            self.logger.warning(f"Could not collect sample dates: {e}")
        
        # Apply the date filter
        date_filtered = all_suspensos.filter(
            (col("dth_inicio") >= start_dt) & (col("dth_inicio") <= end_dt)
        )
        
        # Step 6: Join with eligible processes
        self.logger.info(f"🔗 Joining with eligible processes...")
        eligible_str = self.eligible_processes_df.select("id_processo").distinct().withColumn("id_str", col("id_processo").cast(StringType()))
        date_filtered = date_filtered.withColumn("id_str", col("id_processo").cast(StringType()))
        eligible_suspensos = date_filtered.join(eligible_str.select("id_str"), on="id_str", how="inner").drop("id_str")
        
        # Step 7: Keep first occurrence per process (earliest dth_inicio)
        window_susp = Window.partitionBy("id_processo").orderBy(asc("dth_inicio"))
        situacao_suspenso_df = (eligible_suspensos
            .withColumn("_rn", row_number().over(window_susp))
            .filter((col("_rn") == 1))
            .drop("_rn")
        )
        
        # Step 8: Exclude julgados
        result_df = situacao_suspenso_df.join(julgadom_df, "id_processo", "left_anti")
        
        # Step 9: Add metadata columns
        result_df_final = result_df.withColumn("variavel", lit(f"susm01")) \
            .withColumn("meta_indicator", lit( f"meta_01")) \
            .withColumn("dth_referencia", lit(self.period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(self.period_start)) \
            .withColumn("periodo_fim", lit(self.period_end))
        
        result_df_final = result_df_final.repartition(400, "id_processo").cache()
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        
        self.logger.info(f"✅ susm01 calculation completed records in {calculation_time:.2f}s")
        return result_df_final

    def _calculate_cnm01(self) -> DataFrame:
        calculation_start = datetime.now()
        logger.info("🚀 Calculating eproc cnm01 (distribution - judgments)")
        
        # Load situacao codes from configuration
        distribution_situacoes = self.config_manager.get_situacao_codes("metas", "distribution_situacoes")
        
        # Load distribution data
        logger.info(f"📂 Loading distribution situacoes: {distribution_situacoes}")
        distr_df = []

        for situacao in distribution_situacoes:
            situacao_df = self.load_situacao_with_id_processo_filter(
                situacao=situacao,
                apply_period_filter=True,
                period_start=self.meta01_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                distr_df.append(situacao_df)
        logger.info(f"📂 Loaded {len(distr_df)} distribution DataFrames")
        if distr_df:
            result_distr_df = distr_df[0]
            for df in distr_df[1:]:
                result_distr_df = result_distr_df.unionByName(df, allowMissingColumns=True)
        else:
            result_distr_df = self.spark.createDataFrame([], self.eligible_processes_df.schema)

        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        result_distr_df = result_distr_df.withColumn("row_number", row_number().over(window)).filter(col("row_number") == 1).drop("row_number")
        if result_distr_df and not result_distr_df.isEmpty():
            # Add metadata and required columns
            result_distr_df = (result_distr_df
                        .withColumn("variavel", lit("cnm01"))
                        .withColumn("meta_indicator", lit("meta_01"))
                        .withColumn("dth_referencia", lit(self.period_end))
                        .withColumn("dth_calculo", current_timestamp())
                        .withColumn("periodo_inicio", lit(self.period_start))
                        .withColumn("periodo_fim", lit(self.period_end))
                        .withColumn("ano", year(col("dth_inicio")))
                        .withColumn("mes", month(col("dth_inicio")))
                        .withColumn("origin_data", lit("meta03")))

            result_distr_df = result_distr_df.repartition(400, "id_processo").cache()

            calculation_time = (datetime.now() - calculation_start).total_seconds()
            self.logger.info(f"✅ cnm01 completed records in {calculation_time:.2f}s")

            return result_distr_df
        else:
            return self._create_empty_result_df()
                
    def _calculate_decm01(self) -> DataFrame:
        """
        Implements DECm01: Sum of occurrences of Procedimento incidental ou cautelar resolvido (140) in the process, first occurrence in the month.
        """
        logger.info("🚀 Calculating decm01 (decisions in month)")
        decm01_codes = self.config_manager.get_situacao_codes("meta_01", "decm01")
        df_decm01 = None
        for code in decm01_codes:
            df = self.load_situacao_with_id_processo_filter(code, apply_period_filter=True, period_start=self.meta01_cutoff_date, period_end=self.period_end)
            if df is not None:
                if df_decm01 is None:
                    df_decm01 = df
                else:
                    df_decm01 = df_decm01.unionByName(df, allowMissingColumns=True)
        if df_decm01 is None:
            logger.warning("No decm01 data found")
            return self.spark.createDataFrame([], StructType([]))
        # Only first occurrence per process
        window = Window.partitionBy("id_processo").orderBy("dth_inicio")
        df_decm01 = df_decm01.withColumn("row_number", row_number().over(window)).filter(col("row_number") == 1).drop("row_number")
        df_decm01 = df_decm01.withColumn("variavel", lit("decm01")) \
                          .withColumn("ano", year(col("dth_inicio"))) \
                          .withColumn("mes", month(col("dth_inicio"))) \
                          .withColumn("origin_data", lit("conhecimento"))
        return self._select_and_order_meta_columns(df_decm01)

    def _calculate_desm01_with_eligible_processes(self) -> DataFrame:
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating desm01 (unsuspended processes for Meta 01)...")

        from pyspark.sql.functions import col, lit, year, month, current_timestamp, to_date, row_number
        from pyspark.sql.window import Window

        # Step 0: Load 25 using all eligible processes by class filtering by meta 01 period
        situacao_25 = self.load_situacao_with_id_processo_filter(
            situacao=25,
            apply_period_filter=True,
            period_start=self.meta01_cutoff_date,
            period_end=self.period_end
        )
        
        # Keep earliest opened 25 in the month per id_processo (first unsuspension).
        # If you prefer latest opened in month, change asc() to desc().
        window_first = Window.partitionBy("id_processo").orderBy(col("dth_inicio").asc())
        first_opened_25 = (situacao_25.withColumn("row_num", row_number().over(window_first))
                        .filter((col("dth_fim").isNull()) & (col("row_num") == 1))
                        .drop("row_num"))
        
        # Step 1: Remove 25 that are in julgadom01
        eproc_export_path = "/opt/parquet/data/prata/eproc2g/meta_cnj2026/meta_01/current"
        julgadom01_path = f"{eproc_export_path}/julgadom01.parquet/"

        julgadom_df = self.spark.read.parquet(julgadom01_path).select("id_processo")
        opened_25_clean = first_opened_25.join(julgadom_df, "id_processo", "left_anti")
        self.logger.info(f"✅ Removed julgados with success")

        # Step 2: Confirm that the processes are pending
        distrib_no_julg_path = f"{eproc_export_path}/cnm01.parquet"
        distrib_no_julg_df = self.spark.read.parquet(distrib_no_julg_path).select("id_processo")

        opened_25_pending = opened_25_clean.join(distrib_no_julg_df, "id_processo", "inner")
        self.logger.info(f"✅ Processos that start pending")
        """
        EPROC not have data in 2024 to evaluate

        # Step 3: Verify status of the register: 88 open and 25 close
        situacao_88_path = f"{meta01_export_path}/pend_88_eproc.parquet"
        situacao_88_df = self.spark.read.parquet(situacao_88_path).select("id_processo")
        situacao_25_path = f"{meta01_export_path}/pend_25_eproc.parquet"
        situacao_25_df = self.spark.read.parquet(situacao_25_path).select("id_processo")
        sit88_open_sit25_close = situacao_88_df.join(situacao_25_df, "id_processo", "inner")
        self.logger.info(f"✅ 88 Pending and 25 Closed")
        """
        # Step 4: Certify that our processes match the result of the Step 3
        final_df = opened_25_pending #.join(sit88_open_sit25_close, "id_processo", "inner")
        self.logger.info(f"✅ Keep just the processes that match all criteria")
        # Add metadata columns and return
        result_df = (final_df
                    .withColumn("variavel", lit("desm01"))
                    .withColumn("meta_indicator", lit("meta_01"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end))
                    .withColumn("ano", year(col("dth_inicio")))
                    .withColumn("mes", month(col("dth_inicio")))
                    .withColumn("origin_data", lit("desm01_unsuspension"))
                    )

        result_df = result_df.repartition(400, "id_processo").cache()
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ desm01 calculation completed records in {calculation_time:.2f}s")

        return result_df

    def _configure_meta01_memory_optimizations(self):
        """Configure Spark for Meta 01 memory optimization with advanced features - ENHANCED WITH CODEC FALLBACK."""
        try:
            spark_conf = self.spark.conf
            
            # Enhanced memory management optimizations for very large datasets
            spark_conf.set("spark.sql.adaptive.enabled", "true")
            spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark_conf.set("spark.sql.adaptive.coalescePartitions.maxPartitionBytes", "64MB")
            spark_conf.set("spark.sql.shuffle.partitions", "800")
            spark_conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            
            # Skew join optimization for better performance with large datasets
            spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
            spark_conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
            
            # Advanced I/O optimizations with predicate pushdown
            spark_conf.set("spark.sql.parquet.filterPushdown", "true")
            spark_conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")
            spark_conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
            spark_conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
            
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
            
            self.logger.info("✅ Applied Meta 01 enhanced memory and performance optimizations with codec fallback")
            
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
        "id_processo", "dth_fim", "dth_inicio", "ident_mov", "id_sigilo", "situacao", "variavel"
    ]
    def _select_and_order_meta_columns(self, df):
        # Functions and types already imported at top level
        default_types = {
            "cod_classe": StringType(),
            "cod_oj": StringType(),
            "cod_proc": StringType(),
            "cod_tip_mov": StringType(),
            "cod_tip_mov_numero": StringType(),
            "id_processo": StringType(),
            "dth_fim": TimestampType(),
            "dth_inicio": TimestampType(),
            "ident_mov": StringType(),
            "id_sigilo": IntegerType(),
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
        try:
            # Set checkpoint directory if not already set
            self.logger.info(f"💾 Creating checkpoint for {name}...")
            df = df.checkpoint(eager=True)
            self.logger.info(f"✅ Checkpoint created for {name}")
            return df
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to checkpoint {name}: {e}")
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
            ("julgadom01", self._calculate_julgadom01_with_eligible_processes),
            ("susm01", self._calculate_susm01_with_eligible_processes),
            ("decm01", self._calculate_decm01),
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
                    # Directly export result with all columns preserved
                    if self._export_eproc_results_locally(result, variable_name):
                        self.logger.info(f"✅ {variable_name} exported")
                        results[variable_name] = 1
                    else:
                        self.logger.error(f"❌ Failed to export {variable_name}")
                    try:
                        result.unpersist(blocking=False)
                    except Exception as unpersist_error:
                        self.logger.warning(f"⚠️ Error unpersisting {variable_name}: {unpersist_error}")
                        return None
                    self._cleanup_memory_aggressive()
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
        
        # Determine success
        successful_calculations = sum(1 for count in results.values() if count > 0)
        total_calculations = len(calculations)
        
        if total_calculations > 0:
            self.logger.info(f"🎉 All {total_calculations} Meta 01 calculations completed!")
            return True
        else:
            self.logger.warning(f"⚠️ {successful_calculations}/{total_calculations} calculations completed")
            return False

    def get_calculation_summary(self) -> Dict[str, int]:
        """Return summary of calculation results."""
        return self.calculation_results.copy()

    def _apply_eproc_to_ejud_schema_mapping(self, df: DataFrame) -> DataFrame:
        """
        Apply EPROC to EJUD schema mapping for downstream compatibility.
        
        This function maps EPROC column names to EJUD expected column names
        to ensure proper datetime field handling in downstream processes.
        
        Args:
            df: EPROC DataFrame to transform
            
        Returns:
            DataFrame: Transformed DataFrame with EJUD-compatible schema
        """
        try:
            # CRITICAL MAPPING: dth_inicio (EPROC) -> dth_inicio (EJUD) for datetime events
            if "dth_inicio" in df.columns and "dth_inicio" not in df.columns:
                df = df.withColumn("dth_inicio", col("dth_inicio"))
                self.logger.info("🔄 Mapped EPROC dth_inicio -> dth_inicio for datetime compatibility")
            
            # Map id_processo (EPROC) -> id_processo (EJUD) for process identification
            if "id_processo" in df.columns and "id_processo" not in df.columns:
                df = df.withColumn("id_processo", col("id_processo"))
                self.logger.info("🔄 Mapped EPROC id_processo -> id_processo for process ID compatibility")
            
            # Map id_sigilo (EPROC) -> nivel_sigilo (EJUD) for privacy level
            if "id_sigilo" in df.columns and "nivel_sigilo" not in df.columns:
                df = df.withColumn("nivel_sigilo", col("id_sigilo"))
                self.logger.info("🔄 Mapped EPROC id_sigilo -> nivel_sigilo for privacy compatibility")
            
            # Map cod_mni_movimento (EPROC) -> cod_tip_mov (EJUD) for movement type
            if "cod_mni_movimento" in df.columns and "cod_tip_mov" not in df.columns:
                df = df.withColumn("cod_tip_mov", col("cod_mni_movimento"))
                self.logger.info("🔄 Mapped EPROC cod_mni_movimento -> cod_tip_mov for movement type compatibility")
            
            # Map cod_mni_complemento (EPROC) -> cod_tip_mov_numero (EJUD) for movement code
            if "cod_mni_complemento" in df.columns and "cod_tip_mov_numero" not in df.columns:
                df = df.withColumn("cod_tip_mov_numero", col("cod_mni_complemento"))
                self.logger.info("🔄 Mapped EPROC cod_mni_complemento -> cod_tip_mov_numero for movement code compatibility")
            """
            # Ensure id_assunto_principal exists (EJUD field not present in EPROC)
            if "id_assunto_principal" not in df.columns:
                df = df.withColumn("id_assunto_principal", lit(None))
                self.logger.info("➕ Added id_assunto_principal as null (EPROC doesn't use processoassunto)")
            """
            return df
            
        except Exception as e:
            self.logger.warning(f"⚠️ Schema mapping failed: {e}")
            return df

    def _export_eproc_results_locally(self, df: DataFrame, variable_name: str) -> bool:
        """
        Export EPROC results directly to prata/eproc2g path structure with proper schema mapping.
        
        Args:
            df: DataFrame to export
            variable_name: Name of the variable for filename
            
        Returns:
            bool: True if export successful
        """
        try:
            # Check if DataFrame is empty first
            record_ = 1 if df is not None else 0
            if record_ == 0:
                self.logger.warning(f"⚠️ {variable_name} DataFrame is empty")
                # Create empty result with proper schema
                df = self._create_empty_result_df()
                # Add required meta columns for compatibility
                df = df.withColumn("variavel", lit(variable_name))
                record_ = 0
            
            # Ensure id_processo is always string for compatibility
            if "id_processo" in df.columns:
                df = df.withColumn("id_processo", col("id_processo").cast(StringType()))
            if "num_processo" in df.columns:
                df = df.withColumn("num_processo", col("num_processo").cast(StringType()))
            
            # Apply EPROC -> EJUD schema mapping for downstream compatibility
            df = self._apply_eproc_to_ejud_schema_mapping(df)
            
            # Use EPROC-specific export path structure (with meta_cnj2026 directory)
            eproc_export_path = "/opt/parquet/data/prata/eproc2g/meta_cnj2026/meta_01"
            current_path = f"{eproc_export_path}/current"
            archive_path = f"{eproc_export_path}/archive"
            
            # Create directories if they don't exist
            import os
            os.makedirs(current_path, exist_ok=True)
            os.makedirs(archive_path, exist_ok=True)
            
            # Define file paths
            current_file = f"{current_path}/{variable_name}.parquet"
            archive_file = f"{archive_path}/{variable_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            
            # Archive previous if needed
            if os.path.exists(current_file):
                try:
                    os.rename(current_file, archive_file)
                    self.logger.info(f"� Archived previous {variable_name}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Archive failed: {e}")
                    # If rename fails, try to remove the old file
                    try:
                        if os.path.isdir(current_file):
                            shutil.rmtree(current_file)
                            self.logger.info(f"🗑️ Removed corrupted {variable_name} directory")
                        else:
                            os.remove(current_file)
                            self.logger.info(f"🗑️ Removed corrupted {variable_name} file")
                    except Exception as remove_error:
                        self.logger.warning(f"⚠️ Failed to remove corrupted file/directory: {remove_error}")
            
            # Smart partitioning based on data size
            if record_ > 1000000:
                partition_ = min(max(record_ // 20000, 200), 800)
            elif record_ > 500000:
                partition_ = min(max(record_ // 10000, 100), 400)
            elif record_ > 100000:
                partition_ = min(max(record_ // 5000, 50), 200)
            else:
                partition_ = 100
            
            if record_ > 0:
                df = df.repartition(partition_)
            
            # CRITICAL FIX: Add 3 hours to datetime columns to correct timezone offset
            # System shows "07/08/2025 19:02:26" but exports "2025-08-07 16:02:26" (3 hours behind)
            # EXCEPTION: julgado (julgadom01) should NOT have timezone conversion applied 
            # because it reads from other variables that already have the conversion applied
            
            # Skip timezone conversion for julgado variables - they already have correct timestamps
            if variable_name.startswith("julgado"):
                self.logger.info(f"⏰ SKIPPING timezone conversion for {variable_name} - already processed")
            else:
                if "dth_inicio" in df.columns:
                    df = df.withColumn("dth_inicio", 
                        expr("dth_inicio + INTERVAL 3 HOURS"))
                    self.logger.info(f"🔄 Added 3 hours to dth_inicio for {variable_name} to correct timezone offset")
                
                if "dth_fim" in df.columns:
                    df = df.withColumn("dth_fim", 
                        expr("CASE WHEN dth_fim IS NOT NULL THEN dth_fim + INTERVAL 3 HOURS ELSE NULL END"))
                    self.logger.info(f"🔄 Added 3 hours to dth_fim for {variable_name} to correct timezone offset")
            
            # Write with proper configuration
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            export_start = datetime.now()
            df.write.mode("overwrite").parquet(current_file)
            export_time = (datetime.now() - export_start).total_seconds()
            
            self.logger.info(f"✅ Exported {variable_name}: {record_:,} records in {export_time:.2f}s")
            self.logger.info(f"📁 Saved to EPROC path: {current_file}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to export {variable_name}: {e}")
            import traceback
            self.logger.error(f"📋 Traceback: {traceback.format_exc()}")
            return False

    def _cleanup_memory_aggressive(self):
        """Aggressive memory cleanup - PRODUCTION PROVEN."""
        try:
            gc.collect()
            sc = self.spark.sparkContext
            self.spark.catalog.clearCache()
            sc._jvm.System.gc()
            self.logger.debug("🧹 Memory cleanup completed")
        except Exception as e:
            self.logger.warning(f"⚠️ Error during memory cleanup: {e}")
        # Remove temporary Spark event/checkpoint dir created at init (best-effort)
        
        return None
        
    def load_situacao_with_id_processo_filter(self, situacao: int, 
    apply_period_filter: bool = False,
    period_start: Optional[date] = None,
    period_end: Optional[date] = None) -> DataFrame:
        from datetime import datetime
        import glob
        import os
        
        empty_schema = StructType([
            StructField("id_processo", StringType(), True),
            StructField("dth_inicio", TimestampType(), True),
            StructField("dth_fim", TimestampType(), True),
            StructField("situacao", IntegerType(), True)
        ])

        self.logger.info(f"🔄 load_situacao_with_id_processo_filter called:")
        self.logger.info(f"   📊 situacao: {situacao}")
        self.logger.info(f"   📅 apply_period_filter: {apply_period_filter}")
        self.logger.info(f"   📅 period_start: {period_start}")
        self.logger.info(f"   📅 period_end: {period_end}")
        self.logger.info(f"   📂 Using situacao_base_path: {self.situacao_base_path}")

        # DEBUG: log available situacoes and eligible_processes summary to help triage missing data
        try:
            avail = self.available_situacoes if hasattr(self, 'available_situacoes') else None
            self.logger.info(f"   📋 available_situacoes (sample up to 50): {str(avail)[:500]}")
        except Exception:
            pass    
        # Ensure we have eligible processes loaded
        if not self.eligible_processes_df:
            self.logger.warning(f"Situacao {situacao} cannot be processed because eligible_processes_df is empty or missing")
            return self.spark.createDataFrame([], empty_schema)

        # New: month-based scan (optimized)
        # Build month range to scan
        try:
            start_date = (period_start or self.period_start)
            end_date = (period_end or self.period_end)
            if start_date is None or end_date is None:
                self.logger.warning("⚠️ period_start/period_end not available for month-based lookup; returning empty DataFrame")
                return self.spark.createDataFrame([], empty_schema)

            # Normalize to first-of-month for iteration
            cur = start_date.replace(day=1)
            months = []
            while cur <= end_date:
                months.append((cur.year, cur.month))
                if cur.month == 12:
                    cur = cur.replace(year=cur.year+1, month=1)
                else:
                    cur = cur.replace(month=cur.month+1)
        except Exception as e:
            self.logger.warning(f"⚠️ Could not determine month range for lookup: {e}")
            return self.spark.createDataFrame([], empty_schema)

        month_dfs = []
        # Minimal columns to read for fast pruning
        probe_cols = ["situacao", "dth_inicio", "dth_fim", "id_processo"]

        for (y, m) in months:
            ym_dir = f"{self.situacao_base_path}/year_month={y}-{m:02d}"

            try:
                # Attempt to read the month directory directly; Spark will raise if missing or unreadable
                base_read = self.spark.read.option("recursiveFileLookup", "true").parquet(ym_dir)

                # Determine which probe columns are available and select them
                available_cols = base_read.columns
                selected_probe_cols = [c for c in probe_cols if c in available_cols]
                if not selected_probe_cols:
                    self.logger.debug(f"ℹ️ No probe columns present in {ym_dir}; skipping")
                    continue

                month_probe = base_read.select(*selected_probe_cols)

                # Filter by situacao
                if "situacao" not in month_probe.columns:
                    self.logger.debug(f"ℹ️ 'situacao' column missing in {ym_dir}; skipping")
                    continue

                filtered = month_probe.filter(col("situacao") == situacao)

                # Apply period filter if requested
                if apply_period_filter and "dth_inicio" in filtered.columns:
                    start_dt = datetime.combine(start_date, datetime.min.time())
                    end_dt = datetime.combine(end_date, datetime.max.time())
                    filtered = filtered.filter((col("dth_inicio") >= start_dt) & (col("dth_inicio") <= end_dt))

                # Cheap presence check: fetch at most one row to decide whether to load full month
                try:
                    has_rows = len(filtered.limit(1).collect()) > 0
                except Exception:
                    has_rows = False

                if has_rows:
                    self.logger.info(f"✅ Month {y}-{m:02d} has records for situacao {situacao}; loading full month data")
                    # Reuse base_read (full month) and filter
                    full_month = base_read
                    if "situacao" in full_month.columns:
                        full_filtered = full_month.filter(col("situacao") == situacao)
                    else:
                        continue

                    # Apply period filter to full data as well
                    if apply_period_filter and "dth_inicio" in full_filtered.columns:
                        start_dt = datetime.combine(start_date, datetime.min.time())
                        end_dt = datetime.combine(end_date, datetime.max.time())
                        full_filtered = full_filtered.filter((col("dth_inicio") >= start_dt) & (col("dth_inicio") <= end_dt))

                    month_dfs.append(full_filtered)
            except Exception as e:
                self.logger.debug(f"ℹ️ Could not read month {ym_dir}: {e}")
                continue

        if not month_dfs:
            self.logger.warning(f"❌ No month data found for situacao {situacao} in range {start_date} to {end_date}")
            return self.spark.createDataFrame([], empty_schema)

        # Union all matched months
        df = month_dfs[0]
        for d in month_dfs[1:]:
            df = df.unionByName(d, allowMissingColumns=True)

        # Filter eligible processes (string join for BigInt)
        eligible_str = self.eligible_processes_df.select("id_processo").distinct().withColumn("id_str", col("id_processo").cast(StringType()))
        df = df.withColumn("id_str", col("id_processo").cast(StringType()))
        df = df.join(eligible_str.select("id_str"), on="id_str", how="inner").drop("id_str")

        # Period filter
        if apply_period_filter and period_start and period_end:
            start_dt = datetime.combine(period_start, datetime.min.time())
            end_dt = datetime.combine(period_end, datetime.min.time())
            self.logger.info(f"📅 Applying period filter: {start_dt} to {end_dt}")

            # SPECIAL DEBUG for situacao 49 (suspenso issue)
            if situacao == 49:
                self.logger.info(f"🔍 SPECIAL DEBUG for situacao 49:")
                self.logger.info(f"   📅 Filtering range: {start_dt} to {end_dt}")
                self.logger.info(f"   📊 Sample dates in your data: 2025-09-12 16:38:04, 2025-09-02 16:59:55")
                self.logger.info(f"   ✅ Sample dates should be INCLUDED if filtering works correctly")

            # Show sample dth_inicio values for debugging
            try:
                sample_dates = df.select("dth_inicio").limit(5).collect()
                self.logger.info("🔍 Sample dth_inicio values:")
                for i, row in enumerate(sample_dates):
                    self.logger.info(f"   {i+1}: {row['dth_inicio']}")

            except Exception as e:
                self.logger.warning(f"Could not collect sample dates: {e}")

            df = df.filter((col("dth_inicio") >= start_dt) & (col("dth_inicio") <= end_dt))

        return df

    def _create_empty_result_df(self) -> DataFrame:
        """Create an empty DataFrame with the standard result schema for Meta 02."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
        
        empty_schema = StructType([
            StructField("id_processo", StringType(), True),
            StructField("dth_inicio", TimestampType(), True),
            StructField("dth_fim", TimestampType(), True),
            StructField("situacao", IntegerType(), True),
            StructField("variavel", StringType(), True)
        ])
        return self.spark.createDataFrame([], empty_schema)
    
def main():
    """Main execution function for production Meta 01 calculations."""
    parser = argparse.ArgumentParser(description='Meta 01 EPROC Production Calculator')
    parser.add_argument('--period_start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--period_end', type=str, help='End date (YYYY-MM-DD)')
    
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
            SparkMetaUtils.get_optimized_spark_config("Meta_01_eproc")
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
        calculator = MetaCNJ2025CalculatorEPROC_Meta01_Production(
            period_start=period_start,
            period_end=period_end,
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
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)
    
    finally:
        # Best-effort cleanup to avoid leftover shuffle races
        # Stop the SparkSession we created for this script
        try:
            if spark is not None:
                safe_stop_spark(spark, logger=logger)
        except Exception:
            logger.warning("Failed to stop Spark gracefully.")
        import time
        time.sleep(1)
        sys.exit(0)

if __name__ == "__main__":
    main()
