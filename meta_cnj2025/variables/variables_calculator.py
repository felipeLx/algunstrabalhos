from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, final
import argparse
import sys
import os
import logging
import glob
import gc
import shutil
from utils.parquet_utils import meta_export_path
# PySpark imports
try:
    from pyspark.sql import SparkSession, DataFrame
    
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

class MetaVariables:
    def __init__(self, spark, config_manager, logger, hybrid_loader, misc, eligible_processes_df, meta_logger,
                 period_start, period_end, meta_cutoff_date, meta_15year_cutoff, meta_number, meta_path=None,
                 loading_mode: str = "dism_filter", **kwargs):
        """
        Initialize MetaVariables for calculating meta indicators.

        Args:
            loading_mode: One of:
                - "dism_filter": Load situacao filtered by dism{meta}.parquet (default, used by meta02)
                - "eligible_processes": Load situacao filtered by eligible_processes_df (used by meta01)
        """
        self.spark = spark
        self.config_manager = config_manager
        self.logger = logger
        self.hybrid_loader = hybrid_loader
        self.misc = misc
        self.eligible_processes_df = eligible_processes_df
        self.meta_logger = meta_logger
        self.period_start = period_start
        self.period_end = period_end
        self.meta_cutoff_date = meta_cutoff_date
        self.meta_15year_cutoff = meta_15year_cutoff
        self.meta_number = meta_number
        self.loading_mode = loading_mode

        if meta_path is None:
            self.logger.info(f"meta_path not provided directly for meta {self.meta_number}, attempting to derive...")
            # The calling class (GenericMetaCalculator) likely has attributes like 'meta02_filter_paths'
            # which are passed in kwargs because of the `**self.__dict__` call pattern.
            config_key = f'meta{self.meta_number:02}_filter_paths'
            meta_path = kwargs.get(config_key)
            
            if not meta_path:
                self.logger.info(f"Could not find '{config_key}' in kwargs, checking config manager...")
                # Fallback to config manager
                meta_path = self.config_manager.get_config('paths', {}).get(config_key)

        if meta_path is None:
            raise ValueError(f"FATAL: meta_path could not be determined for meta {self.meta_number}. It must be passed as 'meta_path' or exist in config as '{config_key}'.")
            
        self.meta_filter_paths = meta_path

        for i, path in enumerate(self.meta_filter_paths):
            self.logger.info(f"   📂 Filter {i+1}: {path}")
            exists = os.path.exists(path)
            self.logger.info(f"      ✅ Exists: {exists}")
            if not exists:
                # Try with workspace root
                workspace_root = Path(__file__).parent
                alt_path = workspace_root / path
                alt_exists = alt_path.exists()
                self.logger.info(f"      🔍 Alternative path: {alt_path} (exists: {alt_exists})")

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
            return self.load_situacao_with_dism_filter(
                situacao, apply_period_filter, period_start, period_end
            )
        elif self.loading_mode == "eligible_processes":
            return self.load_situacao_with_eligible_processes_filter(
                situacao, apply_period_filter, period_start, period_end
            )
        else:
            raise ValueError(f"Unknown loading_mode: {self.loading_mode}")

    def load_situacao_with_eligible_processes_filter(self, situacao: int,
                                                      apply_period_filter: bool = False,
                                                      period_start: Optional[date] = None,
                                                      period_end: Optional[date] = None) -> DataFrame:
        """
        Load situacao filtered by eligible_processes_df directly (meta_01 pattern).

        Args:
            situacao: Situacao code to load
            apply_period_filter: Whether to apply period filtering
            period_start: Start date for period filtering
            period_end: End date for period filtering

        Returns:
            Spark DataFrame with filtered situacao data (only eligible processes)
        """
        from pyspark.sql.functions import col
        from pyspark.sql.types import LongType

        # Get available situacoes from config
        available_situacoes = self.config_manager.get_available_situacoes("ejud")

        if situacao not in available_situacoes:
            self.logger.warning(f"Situacao {situacao} not available - returning empty DataFrame")
            return self.misc._create_empty_result_df()

        if self.eligible_processes_df is None:
            self.logger.warning(f"No eligible processes available (DataFrame is None) - returning empty DataFrame")
            return self.misc._create_empty_result_df()

        try:
            eligible_processes_list = self.eligible_processes_df.select("coddoc").distinct()
            eligible_processes_list = eligible_processes_list.withColumn("coddoc", col("coddoc").cast(LongType()))
            self.logger.info(f"📊 Loading situacao {situacao} with eligible_processes filter")
        except Exception as e:
            self.logger.error(f"Error preparing eligible processes: {e}")
            return self.misc._create_empty_result_df()

        try:
            df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=eligible_processes_list,
                apply_period_filter=apply_period_filter,
                period_start=period_start,
                period_end=period_end
            )

            if df is not None:
                self.logger.info(f"✅ Successfully loaded situacao {situacao}")
                return df
            else:
                self.logger.warning(f"⚠️ No data for situacao {situacao}")
                return self.misc._create_empty_result_df()

        except Exception as e:
            self.logger.error(f"❌ Failed to load situacao {situacao}: {e}")
            return self.misc._create_empty_result_df()

    def _calculate_primeirasentm_with_eligible_processes(self) -> DataFrame:
        """
        Calculate primeirasentm (first sentences) using Meta eligible processes.
        Ensures intersection filter and date cut are applied.

        For meta_01 (eligible_processes mode): Applies historical check to ensure
        sentence did NOT happen before the current year.
        """
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating primeirasentm (first sentences for Meta)...")

        # Load sentencing situacao codes from configuration
        prim_sent_situacoes = self.config_manager.get_situacao_codes("metas", "prim_sent_situacoes")
        self.logger.info(f"📂 Loading sentencing situacoes: {prim_sent_situacoes}")

        sentencing_dfs = []
        for situacao in prim_sent_situacoes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
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
        first_sentences = (eligible_sentences
                        .withColumn("sentence_rank",
                                    row_number().over(
                                        Window.partitionBy("coddoc")
                                            .orderBy("dthrmov")
                                    ))
                        .filter(col("sentence_rank") == 1)
                        .drop("sentence_rank"))

        # Historical check for meta_01 (eligible_processes mode)
        # Exclude processes that had sentence before the current year
        if self.loading_mode == "eligible_processes":
            self.logger.info(f"📂 Loading historical sentences (1988 to {self.meta_cutoff_date}) for exclusion...")
            current_year_coddocs = first_sentences.select("coddoc").distinct()

            historical_sent_dfs = []
            for situacao in prim_sent_situacoes:
                hist_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                    situacao=situacao,
                    coddoc_list=current_year_coddocs,
                    apply_period_filter=True,
                    period_start=date(1988, 1, 1),
                    period_end=self.meta_cutoff_date - timedelta(days=1)
                )
                if hist_df is not None:
                    historical_sent_dfs.append(hist_df)

            if historical_sent_dfs:
                historical_sent_df = historical_sent_dfs[0]
                for df in historical_sent_dfs[1:]:
                    historical_sent_df = historical_sent_df.unionByName(df, allowMissingColumns=True)

                historical_coddocs = historical_sent_df.select("coddoc").distinct()
                first_sentences = first_sentences.join(historical_coddocs, "coddoc", "left_anti")
                self.logger.info("✅ Applied historical exclusion (sentence in current year BUT NOT before)")
            else:
                self.logger.info("📂 No historical sentences found - all current year sentences are valid")

        # Add metadata
        result_df = first_sentences.withColumn("variavel", lit(f"primeirasentm{self.meta_number}")) \
                                .withColumn("meta_indicator", lit( f"meta_{self.meta_number}")) \
                                .withColumn("dth_referencia", lit(self.period_end)) \
                                .withColumn("dth_calculo", current_timestamp()) \
                                .withColumn("periodo_inicio", lit(self.period_start)) \
                                .withColumn("periodo_fim", lit(self.period_end))

        # Persist using size-aware helper (avoids arbitrary high partition counts and duplicate counts)
        result_df = self.misc._persist_dataframe_safely(result_df)
        return result_df

    def _calculate_baixm_with_eligible_processes(self) -> DataFrame:
        """
        Calculate baixm (archived processes) using Meta eligible processes.
        Ensures intersection filter and date cut are applied.
        """
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating baixm (archived processes for Meta)...")
        
        # Load baixa situacao codes from configuration
        baixa_situacoes = self.config_manager.get_situacao_codes( f"metas", "baixa_situacoes")
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
                baixa_dfs.append(situacao_df)
        
        if not baixa_dfs:
            self.logger.error("❌ No baixa situacao files loaded with 2026 movements!")
            return None
        
        # Union all baixa situacoes
        all_baixas = baixa_dfs[0]
        for df in baixa_dfs[1:]:
            all_baixas = all_baixas.unionByName(df, allowMissingColumns=True)

        # Load additional condition: Pendente (88) processes that end and transition to specific situacoes
        self.logger.info("📂 Loading Pendente (88) processes for transition analysis...")
        situacao_88_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
            88,
            coddoc_list=self.eligible_processes_df,
            apply_period_filter=True,
            period_start=self.period_start,
            period_end=self.period_end
        )
        
        transition_situacoes = [26, 91, 65]  # Execução não criminal, Liquidação Iniciada, Fase processual iniciada
        transition_dfs = []
        
        for situacao in transition_situacoes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                transition_dfs.append(situacao_df)
        
        # Find Pendente (88) processes that transition to these situacoes
        additional_baixas = None
        if situacao_88_df is not None and transition_dfs:
            self.logger.info("🔗 Finding Pendente (88) → transition matches...")
            
            # Union all transition situacoes
            all_transitions = transition_dfs[0]
            for df in transition_dfs[1:]:
                all_transitions = all_transitions.unionByName(df, allowMissingColumns=True)
            
            # Find matches: situacao 88 dth_fim = transition dthrmov AND same coddoc
            matched_88s = situacao_88_df.alias("s88").join(
                all_transitions.alias("trans").select("coddoc", "dthrmov"),
                (col("s88.coddoc") == col("trans.coddoc")) & 
                (col("s88.dth_fim") == col("trans.dthrmov")),
                "inner"
            ).select("s88.*")
            
            # Filter matched_88s to only include transitions that happened in 2025
            if matched_88s is not None:
                additional_baixas = matched_88s    
            else:
                self.logger.info("📊 No Pendente (88) transitions found in 2026 after filtering")

        self.logger.info("✅ Data already filtered by eligible processes via predicate pushdown")
        eligible_baixas = all_baixas.unionByName(additional_baixas, allowMissingColumns=True) # if additional_baixas is not None else all_baixas

        # Deduplicate by process (keep first occurrence)
        eligible_baixas = (eligible_baixas
                        .withColumn("row_number",
                                    row_number().over(
                                        Window.partitionBy("coddoc")
                                            .orderBy("dthrmov")
                                    ))
                        .filter(col("row_number") == 1)
                        .drop("row_number"))

        # Historical check for meta_01 (eligible_processes mode)
        # Exclude processes that had baixa before the current year
        if self.loading_mode == "eligible_processes":
            self.logger.info(f"📂 Loading historical baixas (1988 to {self.meta_cutoff_date}) for exclusion...")
            baixa_situacoes = self.config_manager.get_situacao_codes(f"metas", "baixa_situacoes")
            current_year_coddocs = eligible_baixas.select("coddoc").distinct()

            historical_baixa_dfs = []
            for situacao in baixa_situacoes:
                hist_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                    situacao=situacao,
                    coddoc_list=current_year_coddocs,
                    apply_period_filter=True,
                    period_start=date(1988, 1, 1),
                    period_end=self.meta_cutoff_date - timedelta(days=1)
                )
                if hist_df is not None:
                    historical_baixa_dfs.append(hist_df)

            if historical_baixa_dfs:
                historical_baixa_df = historical_baixa_dfs[0]
                for df in historical_baixa_dfs[1:]:
                    historical_baixa_df = historical_baixa_df.unionByName(df, allowMissingColumns=True)

                historical_coddocs = historical_baixa_df.select("coddoc").distinct()
                eligible_baixas = eligible_baixas.join(historical_coddocs, "coddoc", "left_anti")
                self.logger.info("✅ Applied historical exclusion (baixa in current year BUT NOT before)")
            else:
                self.logger.info("📂 No historical baixas found - all current year baixas are valid")

        # Add metadata
        result_df = eligible_baixas.withColumn("variavel", lit(f"baixm{self.meta_number}")) \
                                .withColumn("meta_indicator", lit( f"meta_{self.meta_number}")) \
                                .withColumn("dth_referencia", lit(self.period_end)) \
                                .withColumn("dth_calculo", current_timestamp()) \
                                .withColumn("periodo_inicio", lit(self.period_start)) \
                                .withColumn("periodo_fim", lit(self.period_end))

        # Persist using size-aware helper
        result_df = self.misc._persist_dataframe_safely(result_df)
        self.logger.info(f"✅ baixm{self.meta_number} calculation completed")

        return result_df

    def _calculate_mpum_with_eligible_processes(self) -> DataFrame:
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
        situacao_codes = [17, 34]
        
        mpum_dfs = []
        for situacao in situacao_codes:
            situacao_df = self.load_situacao_unified(
                situacao,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
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
        eligible_mpums = all_mpums.filter(col("cod_tip_mov").isin([11423, 11424, 11425, 12476, 12479, 15486, 15487, 15488]))

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

        # Historical check for meta_01 (eligible_processes mode)
        # Exclude processes that had mpu before the current year
        if self.loading_mode == "eligible_processes":
            self.logger.info(f"📂 Loading historical mpum (1988 to {self.meta_cutoff_date}) for exclusion...")
            situacao_codes = [17, 34]
            valid_movements = [11423, 11424, 11425, 12476, 12479, 15486, 15487, 15488]
            current_year_coddocs = eligible_mpums.select("coddoc").distinct()

            historical_mpu_dfs = []
            for situacao in situacao_codes:
                hist_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                    situacao=situacao,
                    coddoc_list=current_year_coddocs,
                    apply_period_filter=True,
                    period_start=date(1988, 1, 1),
                    period_end=self.meta_cutoff_date - timedelta(days=1)
                )
                if hist_df is not None:
                    # Apply the same movement filter as current year
                    hist_df = hist_df.filter(col("cod_tip_mov").isin(valid_movements))
                    historical_mpu_dfs.append(hist_df)

            if historical_mpu_dfs:
                historical_mpu_df = historical_mpu_dfs[0]
                for df in historical_mpu_dfs[1:]:
                    historical_mpu_df = historical_mpu_df.unionByName(df, allowMissingColumns=True)

                historical_coddocs = historical_mpu_df.select("coddoc").distinct()
                eligible_mpums = eligible_mpums.join(historical_coddocs, "coddoc", "left_anti")
                self.logger.info("✅ Applied historical exclusion (mpu in current year BUT NOT before)")
            else:
                self.logger.info("📂 No historical mpum found - all current year mpums are valid")

        # Add metadata
        result_df = eligible_mpums.withColumn("variavel", lit(f"mpum{self.meta_number}")) \
                                 .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
                                 .withColumn("dth_referencia", lit(self.period_end)) \
                                 .withColumn("dth_calculo", current_timestamp()) \
                                 .withColumn("periodo_inicio", lit(self.period_start)) \
                                 .withColumn("periodo_fim", lit(self.period_end))

        self.logger.info(f"✅ mpum{self.meta_number} calculation completed")

        return result_df
    
    def _calculate_julgadom_with_eligible_processes(self) -> DataFrame:
        """
        Calculate julgadom (judged processes) using Meta eligible processes.
        """
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when, month
        from pyspark.sql.window import Window
        from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
        )
        calculation_start = datetime.now()
        self.logger.info("🚀 Calculating julgadom...")
        
        # Collect results from constituent variables
        constituent_results = []
        _meta_export_path = meta_export_path( f"meta_{self.meta_number}", "ejud")
        
        # Load constituent variables from parquet
        for variable in [f"primeirasentm{self.meta_number}", f"baixm{self.meta_number}", f"mpum{self.meta_number}"]:
            parquet_path = f"{_meta_export_path}/{variable}.parquet"
            df = self.misc._load_constituent_variable_from_parquet(parquet_path, variable)
            if df is not None:
                constituent_results.append(df)
        
        if not constituent_results:
            self.logger.error("❌ No constituent results found for julgadom!")
            return None
        
        # Union all constituent results
        all_judged = constituent_results[0]
        for df in constituent_results[1:]:
            all_judged = all_judged.unionByName(df, allowMissingColumns=True)

        if all_judged is not None:
            # Check if the situacao is 17 so the movement must match:
            valid_codes = [11423, 11424, 11425, 12476, 12479, 15486, 15487, 15488]
            all_judged = all_judged.filter(
                (col("situacao") != 17) |
                (col("cod_tip_mov").isin(valid_codes) & (col("situacao") == 17))
            )


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
                    .withColumn("variavel", lit(f"julgadom{self.meta_number}"))
                    .withColumn("meta_indicator", lit( f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))
        
        result_df = self.misc._persist_dataframe_safely(result_df, partition_by="coddoc")

        self.logger.info(f"✅ julgadom completed")        
        return result_df

    def _calculate_susm_with_eligible_processes(self, require_dism_or_desm_union: bool = False) -> DataFrame:
        """
        Calculate SUSM (suspended processes).

        Args:
            require_dism_or_desm_union: If True (meta_01 pattern), susm must be in cnm OR desm.
                                        If False (meta_02 pattern), uses implicit dism filter.
        """
        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating susm{self.meta_number} (refactored logic)...")

        from pyspark.sql.functions import (
            col, lit, current_timestamp,
            row_number, year, month, trim, desc, asc
        )
        from pyspark.sql.window import Window
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType,
            TimestampType, DateType, LongType, DoubleType
        )
        from utils.parquet_utils import meta_export_path as get_meta_export_path

        _meta_export_path = get_meta_export_path(f"meta_{self.meta_number}", "ejud")
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}.parquet"
        sit_schema = StructType([
            StructField("coddoc", LongType(), True)
        ])

        try:
            julgadom_df = (self.spark.read.schema(sit_schema).parquet(julgadom_path)
                          .select("coddoc").distinct()
                          .withColumn("coddoc", col("coddoc").cast("long")))
        except Exception as e:
            self.logger.warning(f"Could not load julgadom for exclusion: {e}")
            julgadom_df = self.spark.createDataFrame([], sit_schema)

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
                susp_dfs.append(situacao_df)

        if not susp_dfs:
            self.logger.error("❌ No suspenso situacao files loaded!")
            return None

        # Union all suspenso situacoes
        all_suspensos = susp_dfs[0]
        for df in susp_dfs[1:]:
            all_suspensos = all_suspensos.unionByName(df, allowMissingColumns=True)

        # Keep first occurrence per coddoc
        window_susp = Window.partitionBy("coddoc").orderBy(asc("dthrmov"))
        situacao_suspenso_df = (all_suspensos
                               .withColumn("_rn", row_number().over(window_susp))
                               .filter(col("_rn") == 1)
                               .drop("_rn"))

        # Meta 01 specific: susm must be in cnm OR desm
        if require_dism_or_desm_union:
            self.logger.info("📂 Applying cnm OR desm union filter (meta_01 pattern)...")
            cnm_path = f"{_meta_export_path}/cnm{self.meta_number}.parquet"
            desm_path = f"{_meta_export_path}/desm{self.meta_number}.parquet"

            try:
                cnm_df = (self.spark.read.schema(sit_schema).parquet(cnm_path)
                         .select("coddoc").distinct()
                         .withColumn("coddoc", col("coddoc").cast("long")))
            except Exception:
                self.logger.warning(f"cnm path not found: {cnm_path} - creating empty DataFrame")
                cnm_df = self.spark.createDataFrame([], sit_schema)

            try:
                desm_df = (self.spark.read.schema(sit_schema).parquet(desm_path)
                          .select("coddoc").distinct()
                          .withColumn("coddoc", col("coddoc").cast("long")))
            except Exception:
                self.logger.warning(f"desm path not found: {desm_path} - creating empty DataFrame")
                desm_df = self.spark.createDataFrame([], sit_schema)

            union_df = cnm_df.unionByName(desm_df, allowMissingColumns=True).distinct()
            situacao_suspenso_df = situacao_suspenso_df.join(union_df, "coddoc", "inner")
            self.logger.info("✅ Applied cnm OR desm union filter")

        # Remove julgados
        result_df = situacao_suspenso_df.join(julgadom_df, "coddoc", "left_anti")

        # Add metadata columns
        result_df_final = (result_df
                          .withColumn("variavel", lit(f"susm{self.meta_number}"))
                          .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                          .withColumn("dth_referencia", lit(self.period_end))
                          .withColumn("dth_calculo", current_timestamp())
                          .withColumn("periodo_inicio", lit(self.period_start))
                          .withColumn("periodo_fim", lit(self.period_end)))

        result_df_final = self.misc._persist_dataframe_safely(result_df_final, partition_by="coddoc")
        self.logger.info(f"✅ susm{self.meta_number} calculation completed")
        return result_df_final

    def _calculate_julgadom_ant(self) -> DataFrame:
        from pyspark.sql.functions import col, lit, year, month, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        from utils.parquet_utils import meta_export_path
        try:
            self.logger.info(f"🚀 Calculating julgadom{self.meta_number}_ant (historical judgments)...")
            meta_path = meta_export_path( f"meta_02", "ejud")
            dism_ant_path = f"{meta_path}/dism02_ant.parquet"
            self.logger.info(f"🚀 Eligibles path: {dism_ant_path}")
            dism_ant_df = self.spark.read.parquet(dism_ant_path).select('coddoc').distinct()
            
            # Load julgado situacao codes from configuration
            julgado_situacoes = self.config_manager.get_situacao_codes( f"metas", "judgment_situacoes")
            self.logger.info(f"📂 Loading julgado situacoes: {julgado_situacoes}")
            
            julgado_dfs = []
            for situacao in julgado_situacoes:
                situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                    situacao,
                    coddoc_list=dism_ant_df,
                    apply_period_filter=True,
                    period_start=self.meta_cutoff_date, 
                    period_end=self.period_end
                )
                if situacao_df is not None:
                    julgado_dfs.append(situacao_df)
            
            if not julgado_dfs:
                self.logger.error("❌ No julgado situacao files loaded with 2026 movements!")
                return None
            
            # Union all julgado situacoes
            all_julgados = julgado_dfs[0]
            for df in julgado_dfs[1:]:
                all_julgados = all_julgados.unionByName(df, allowMissingColumns=True)
            
            tab_julgamento_path = f"/opt/parquet/data/prata/ejud/tab_julgamento"
            julgamento_df = self.spark.read.parquet(tab_julgamento_path).filter(
                (col("dthrmov") >= self.meta_cutoff_date) &
                (col("dthrmov") <= self.period_end)
            )
            w = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
            julgamento_df = julgamento_df.join(dism_ant_df, "coddoc", "inner")
            # Keep the full latest judgment row per coddoc (preserve all joined columns such as tab_xml_andamento)
            julgadom_latest = (
                julgamento_df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
            )
            
            w = Window.partitionBy("coddoc").orderBy("dthrmov")
            # Keep the full latest judgment row per coddoc (preserve all joined columns such as tab_xml_andamento)
            julgadom_latest = (
                all_julgados.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
            )
            
            # Remove movements from situacao 17
            if all_julgados is not None:
                # Check if the situacao is 17 so the movement must match:
                valid_codes = [11423, 11424, 11425, 12476, 12479, 15486, 15487, 15488]
                julgadom_latest = all_julgados.filter(
                    (col("situacao") != 17) |
                    (col("cod_tip_mov").isin(valid_codes) & (col("situacao") == 17))
                )
            # Preserve all columns from the latest judgment row
            result_df = (
                julgadom_latest
                .withColumn("variavel", lit(f"julgadom{self.meta_number}_ant"))
                .withColumn("meta_indicator", lit( f"meta_{self.meta_number}")) \
                .withColumn("dth_referencia", lit(self.period_end)) \
                .withColumn("dth_calculo", current_timestamp()) \
                .withColumn("periodo_inicio", lit(self.period_start)) \
                .withColumn("periodo_fim", lit(self.period_end))
            )

            result_df = self.misc._persist_dataframe_safely(result_df, partition_by="coddoc")
            return result_df
        except Exception as e:
            self.logger.error(f"❌ Error in julgadom_ant calculation: {e}")
            return self.misc._create_empty_result_df()
        
    def _calculate_susm_ant(self) -> DataFrame:
        self.logger.info("🎯 Calculating susm (suspended processes) with complete Meta business rule...")
        
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
        )
        calculation_start = datetime.now()
        # Step 1: eligible_coddocs = dism minus julgadom
        _meta_export_path = meta_export_path( f"meta_{self.meta_number}", "ejud")
        dism_path = f"{_meta_export_path}/dism{self.meta_number}_ant.parquet"
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}_ant.parquet"
        sit_schema = StructType([
            StructField("coddoc", LongType(), True)
        ])
        # Read dism & julgadom with explicit schema and cast coddoc to long
        dism_df = self.spark.read.schema(sit_schema).parquet(dism_path).select("coddoc").distinct()
        julgadom_df = self.spark.read.schema(sit_schema).parquet(julgadom_path).select("coddoc").distinct()

        # Compute eligible = dism \ julgadom (DataFrame left_anti)
        eligible_coddocs_df = dism_df.join(julgadom_df, on="coddoc", how="left_anti")

        susp_situacoes = self.config_manager.get_situacao_codes( f"metas", "suspenso_situacoes")
        self.logger.info(f"📂 Loading suspenso situacoes: {susp_situacoes}")

        susp_dfs = []
        for situacao in susp_situacoes:
            situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao,
                coddoc_list=eligible_coddocs_df,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date, 
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

        # 9) Add metadata columns to the full-row result DataFrame
        result_df = situacao_suspenso_df.withColumn("variavel", lit(f"susm{self.meta_number}_ant")) \
            .withColumn("meta_indicator", lit( f"meta_{self.meta_number}")) \
            .withColumn("dth_referencia", lit(self.period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(self.period_start)) \
            .withColumn("periodo_fim", lit(self.period_end))
        
        #result_df = result_df.repartition(400, "coddoc").cache()
        self.logger.info(f"✅ susm{self.meta_number} calculation completed")
        return result_df
 
    def _calculate_desm_ant_with_eligible_processes(self) -> DataFrame:
        from pyspark.sql.functions import col, lit, year, month, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating desm{self.meta_number} (unsuspended processes for Meta)...")
        # Step 0: Load 25 using all eligible processes by class filtering by meta period
        _meta_export_path = meta_export_path( f"meta_02", "ejud")
        dism_ant_path = f"{_meta_export_path}/dism02_ant.parquet"
        dism_ant_df = self.spark.read.parquet(dism_ant_path).select("coddoc").distinct()
        
        situacao_25 = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=25,
            coddoc_list=dism_ant_df,
            apply_period_filter=True,
            period_start=self.meta_cutoff_date,
            period_end=self.period_end
        )        
        
        # Keep earliest opened 25 in the month per coddoc (first unsuspension).
        window_first = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        first_25 = (situacao_25.withColumn("row_num", row_number().over(window_first))
                        .filter((col("row_num") == 1))
                        .drop("row_num"))
        first_opened_25 = first_25.filter((col("dth_fim").isNull()))
        
        # Step 1.b: Remove julgado_ant
        julgadom_old_path = f"{_meta_export_path}/julgadom{self.meta_number}_ant.parquet"

        julgadom_old_df = self.spark.read.parquet(julgadom_old_path).select("coddoc")
        opened_25_clean = first_opened_25.join(julgadom_old_df, "coddoc", "left_anti")
        self.logger.info(f"✅ Removed julgados with success")

        final_df = opened_25_clean
        # Add metadata columns and return
        result_df = (final_df
                    .withColumn("variavel", lit(f"desm{self.meta_number}"))
                    .withColumn("meta_indicator", lit( f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end))
                    )

        self.logger.info(f"✅ des{self.meta_number} calculation completed")
        return result_df

    def _calculate_cnm(self) -> DataFrame:
        """
        Calculate CNM (distribution) - used by meta_01.
        Loads distribution situacoes and keeps first occurrence per process.

        IMPORTANT: Applies historical check to ensure distribution did NOT happen
        before the current year. Only processes with distribution in the current year
        AND no distribution before are counted.
        """
        from pyspark.sql.functions import col, lit, year, month, current_timestamp, row_number
        from pyspark.sql.window import Window

        calculation_start = datetime.now()
        self.logger.info(f"🚀 Calculating cnm{self.meta_number} (distribution with historical check)")

        # Load situacao codes from configuration
        distribution_situacoes = self.config_manager.get_situacao_codes("metas", "distribution_situacoes")
        self.logger.info(f"📂 Loading distribution situacoes: {distribution_situacoes}")

        # Step 1: Load current year distributions (meta_cutoff_date to period_end)
        distr_dfs = []
        for situacao in distribution_situacoes:
            situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=self.eligible_processes_df,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if situacao_df is not None:
                distr_dfs.append(situacao_df)

        self.logger.info(f"📂 Loaded {len(distr_dfs)} distribution DataFrames for current year")

        if not distr_dfs:
            self.logger.warning("⚠️ No distribution data found for current year")
            return self.misc._create_empty_result_df()

        result_distr_df = distr_dfs[0]
        for df in distr_dfs[1:]:
            result_distr_df = result_distr_df.unionByName(df, allowMissingColumns=True)

        # Keep first occurrence per process in current year
        window = Window.partitionBy("coddoc").orderBy("dthrmov")
        result_distr_df = (result_distr_df
                          .withColumn("row_number", row_number().over(window))
                          .filter(col("row_number") == 1)
                          .drop("row_number"))

        if result_distr_df is None or result_distr_df.isEmpty():
            self.logger.warning("⚠️ CNM returned no results after deduplication")
            return self.misc._create_empty_result_df()

        # Step 2: Historical check - exclude processes that had distribution before cutoff
        self.logger.info(f"📂 Loading historical distributions (1988 to {self.meta_cutoff_date}) for exclusion...")

        # Get coddocs from current year results to use as filter
        current_year_coddocs = result_distr_df.select("coddoc").distinct()

        historical_distr_dfs = []
        for situacao in distribution_situacoes:
            hist_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=current_year_coddocs,
                apply_period_filter=True,
                period_start=date(1988, 1, 1),
                period_end=self.meta_cutoff_date - timedelta(days=1)  # Day before cutoff
            )
            if hist_df is not None:
                historical_distr_dfs.append(hist_df)

        if historical_distr_dfs:
            self.logger.info(f"📂 Found {len(historical_distr_dfs)} historical distribution DataFrames")

            # Union all historical distributions
            historical_distr_df = historical_distr_dfs[0]
            for df in historical_distr_dfs[1:]:
                historical_distr_df = historical_distr_df.unionByName(df, allowMissingColumns=True)

            # Get distinct coddocs that had distribution before
            historical_coddocs = historical_distr_df.select("coddoc").distinct()

            # Exclude processes that had distribution before the current year
            result_distr_df = result_distr_df.join(historical_coddocs, "coddoc", "left_anti")
            self.logger.info("✅ Applied historical exclusion filter (distribution in current year BUT NOT before)")
        else:
            self.logger.info("📂 No historical distributions found - all current year distributions are valid")

        if result_distr_df is None or result_distr_df.isEmpty():
            self.logger.warning("⚠️ CNM returned no results after historical exclusion")
            return self.misc._create_empty_result_df()

        # Add metadata
        result_df = (result_distr_df
                    .withColumn("variavel", lit(f"cnm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end))
                    .withColumn("ano", year(col("dthrmov")))
                    .withColumn("mes", month(col("dthrmov"))))

        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ cnm{self.meta_number} completed in {calculation_time:.2f}s")

        return result_df

    def _calculate_decm(self) -> DataFrame:
        """
        Calculate DECM (decisions in month) - used by meta_01.
        Sum of occurrences of Procedimento incidental ou cautelar resolvido (140),
        first occurrence per process.
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number
        from pyspark.sql.window import Window

        self.logger.info(f"🚀 Calculating decm{self.meta_number} (decisions in month)")

        decm_codes = self.config_manager.get_situacao_codes("metas", "decm_situacoes")
        self.logger.info(f"📂 Loading decm situacoes: {decm_codes}")

        df_decm = None
        for code in decm_codes:
            df = self.load_situacao_unified(
                code,
                apply_period_filter=True,
                period_start=self.meta_cutoff_date,
                period_end=self.period_end
            )
            if df is not None:
                if df_decm is None:
                    df_decm = df
                else:
                    df_decm = df_decm.unionByName(df, allowMissingColumns=True)

        if df_decm is None:
            self.logger.warning(f"No decm{self.meta_number} data found")
            return self.misc._create_empty_result_df()

        # Only first occurrence per process
        window = Window.partitionBy("coddoc").orderBy("dthrmov")
        df_decm = (df_decm
                   .withColumn("row_number", row_number().over(window))
                   .filter(col("row_number") == 1)
                   .drop("row_number"))

        # Add metadata
        result_df = (df_decm
                    .withColumn("variavel", lit(f"decm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        self.logger.info(f"✅ decm{self.meta_number} calculation completed")
        return result_df

    def _calculate_desm_with_pending_check(self) -> DataFrame:
        """
        Calculate DESM (unsuspended processes) with pending status verification - used by meta_01.
        Unlike desm_ant, this checks if processes were pending at period start
        by verifying closed 25 AND opened 88 at cutoff date.
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number
        from pyspark.sql.window import Window

        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating desm{self.meta_number} (unsuspended processes with pending check)...")

        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")

        # Step 0: Load situacao 25 using eligible processes filtered by meta period
        situacao_25 = self.load_situacao_unified(
            situacao=25,
            apply_period_filter=True,
            period_start=self.meta_cutoff_date,
            period_end=self.period_end
        )

        if situacao_25 is None or situacao_25.isEmpty():
            self.logger.warning("No situacao 25 data found")
            return self.misc._create_empty_result_df()

        # Keep earliest opened 25 per coddoc (first unsuspension)
        window_first = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        first_25 = (situacao_25
                    .withColumn("row_num", row_number().over(window_first))
                    .filter(col("row_num") == 1)
                    .drop("row_num"))
        first_opened_25 = first_25.filter(col("dth_fim").isNull())

        if first_opened_25 is None or first_opened_25.isEmpty():
            self.logger.warning("No opened situacao 25 found")
            return self.misc._create_empty_result_df()

        # Step 1: Remove processes that are in julgadom
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}.parquet"
        try:
            julgadom_df = self.spark.read.parquet(julgadom_path).select("coddoc")
            opened_25_clean = first_opened_25.join(julgadom_df, "coddoc", "left_anti")
            self.logger.info("✅ Removed julgados successfully")
        except Exception as e:
            self.logger.warning(f"Could not load julgadom for exclusion: {e}")
            opened_25_clean = first_opened_25

        first_25_coddoc = opened_25_clean.select("coddoc")

        # Step 2: Confirm processes were pending at the start of the period
        # Load historical 25 (before cutoff)
        closed_25 = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=25,
            coddoc_list=first_25_coddoc,
            apply_period_filter=True,
            period_start=date(1988, 1, 1),
            period_end=self.meta_cutoff_date
        )

        if closed_25 is not None and not closed_25.isEmpty():
            window_desc = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
            last_25 = (closed_25
                       .withColumn("row_num", row_number().over(window_desc))
                       .filter(col("row_num") == 1)
                       .drop("row_num"))
            last_25 = last_25.filter(col("dth_fim").isNotNull()).select("coddoc")
        else:
            last_25 = self.spark.createDataFrame([], "coddoc: long")

        # Load historical 88 (before cutoff)
        opened_88 = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=88,
            coddoc_list=first_25_coddoc,
            apply_period_filter=True,
            period_start=date(1988, 1, 1),
            period_end=self.meta_cutoff_date
        )

        if opened_88 is not None and not opened_88.isEmpty():
            window_desc = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
            last_88 = (opened_88
                       .withColumn("row_num", row_number().over(window_desc))
                       .filter(col("row_num") == 1)
                       .drop("row_num"))
            last_88 = last_88.filter(col("dth_fim").isNull()).select("coddoc")
        else:
            last_88 = self.spark.createDataFrame([], "coddoc: long")

        # Union pending = processes with closed 25 AND opened 88
        union_pending = last_25.join(last_88, "coddoc", "inner")

        # Check if the opened 25 was pending
        final_df = opened_25_clean.join(union_pending, "coddoc", "inner")

        # Add metadata
        result_df = (final_df
                    .withColumn("variavel", lit(f"desm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end)))

        calculation_time = (datetime.now() - calculation_start).total_seconds()
        self.logger.info(f"✅ desm{self.meta_number} calculation completed in {calculation_time:.2f}s")

        return result_df

    def load_situacao_with_dism_filter(self, situacao: int, 
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
        from pyspark.sql.functions import col, lit, year, month, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        _meta_export_path = meta_export_path( f"meta_{self.meta_number}", "ejud")
        path = f"{_meta_export_path}/dism{self.meta_number}.parquet"
        dism_processes_df = self.spark.read.parquet(path)
        dism_processes_df = dism_processes_df.withColumn("coddoc", col("coddoc").cast("long"))
        dism_processes_list = dism_processes_df.select("coddoc").distinct()
            
        try:            
            df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=dism_processes_list, 
                apply_period_filter=True,
                period_start=period_start,
                period_end=period_end
            )
            
            if df is not None:
                self.logger.info(f"✅ Successfully loaded situacao {situacao}")
                return df
            else:
                self.logger.warning(f"⚠️ No data for situacao {situacao}")
                return self.misc._create_empty_result_df()
                
        except Exception as e:
            self.logger.error(f"❌ Failed to load situacao {situacao}: {e}")