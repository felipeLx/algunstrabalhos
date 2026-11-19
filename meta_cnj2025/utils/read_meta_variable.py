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
from utils.parquet_utils import meta_export_path
# PySpark imports
try:
    from pyspark.sql import SparkSession, DataFrame
    
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

class MetaVariables:
    def __init__(self, spark, config_manager, logger, hybrid_loader, misc, eligible_processes_df, meta_path, meta_logger, 
                 period_start, period_end, meta_cutoff_date, meta_15year_cutoff, meta_number):
        self.spark = spark
        self.config_manager = config_manager
        self.logger = logger
        self.hybrid_loader = hybrid_loader
        self.misc = misc
        self.eligible_processes_df = eligible_processes_df
        self.meta_filter_paths = meta_path
        self.meta_logger = meta_logger
        self.period_start = period_start
        self.period_end = period_end
        self.meta_cutoff_date = meta_cutoff_date
        # Add 1 day on the self.meta_cutoff_date
        self.meta_cutoff_date += timedelta(days=1)
        self.meta_15year_cutoff = meta_15year_cutoff
        self.meta_number = meta_number
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
           
    def _calculate_primeirasentm_with_eligible_processes(self) -> DataFrame:
        """
        Calculate primeirasentm (first sentences) using Meta eligible processes.
        Ensures intersection filter and date cut are applied.
        """
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating primeirasentm (first sentences for Meta)...")
        
        # Load sentencing situacao codes from configuration
        sentencing_situacoes = self.config_manager.get_situacao_codes("metas", "sentencing_situacoes")
        self.logger.info(f"📂 Loading sentencing situacoes: {sentencing_situacoes}")
        
        sentencing_dfs = []
        for situacao in sentencing_situacoes:
            situacao_df = self.load_situacao_with_dism_filter(
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
        
        # Add metadata
        result_df = first_sentences.withColumn("variavel", lit(f"primeirasentm{self.meta_number}")) \
                                .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
                                .withColumn("dth_referencia", lit(self.period_end)) \
                                .withColumn("dth_calculo", current_timestamp()) \
                                .withColumn("periodo_inicio", lit(self.period_start)) \
                                .withColumn("periodo_fim", lit(self.period_end))
        
        # Optimize and cache using configured partition count
        result_df = repartition_and_cache(result_df, keys='situacao', config_manager=self.config_manager)
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        record_count = result_df.count()
        self.meta_logger.log_variable_completion("primeirasentm", record_count, calculation_time)
        self.logger.info(f"✅ primeirasentm calculation completed: {record_count:,} records in {calculation_time:.2f}s")
        
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
        baixa_situacoes = self.config_manager.get_situacao_codes(f"meta_{self.meta_number}", "baixa_situacoes")
        self.logger.info(f"📂 Loading baixa situacoes: {baixa_situacoes}")
        
        baixa_dfs = []
        for situacao in baixa_situacoes:
            situacao_df = self.load_situacao_with_dism_filter(
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
        situacao_88_df = self.load_situacao_with_dism_filter(
            88,
            apply_period_filter=True,
            period_start=self.period_start,
            period_end=self.period_end
        )
        
        transition_situacoes = [26, 91, 65]  # Execução não criminal, Liquidação Iniciada, Fase processual iniciada
        transition_dfs = []
        
        for situacao in transition_situacoes:
            situacao_df = self.load_situacao_with_dism_filter(
                situacao,
                apply_period_filter=True,
                period_start=self.period_start,  
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
        
        # Add metadata
        result_df = eligible_baixas.withColumn("variavel", lit(f"baixm{self.meta_number}")) \
                                .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
                                .withColumn("dth_referencia", lit(self.period_end)) \
                                .withColumn("dth_calculo", current_timestamp()) \
                                .withColumn("periodo_inicio", lit(self.period_start)) \
                                .withColumn("periodo_fim", lit(self.period_end))
        
        # Optimize and cache
        result_df = result_df.repartition(400, "situacao").cache()
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        record_count = result_df.count()
        
        self.meta_logger.log_variable_completion(f"baixm{self.meta_number}", record_count, calculation_time)
        self.logger.info(f"✅ baixm{self.meta_number} calculation completed: {record_count:,} records in {calculation_time:.2f}s")
        
        return result_df

    def _calculate_mpum_with_eligible_processes(self) -> DataFrame:
        """
        Implements mpum: Soma de ocorrências das situações Decisão proferida (17) e Medida protetiva homologada ou revogada (34),
        restrita aos movimentos 11423, 11424, 11425, 12476, 12479, 15486, 15487 e 15488, primeira ocorrência no processo no mês analisado.
        """
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
        )
        calculation_start = datetime.now()
        self.logger.info("🚀 Calculating mpum (medidas protetivas e decisões proferidas no mês para Meta)...")
        situacao_codes = self.config_manager.get_situacao_codes("metas", "mpum_situacoes")

        mpum_dfs = []
        for situacao in situacao_codes:
            situacao_df = self.load_situacao_with_dism_filter(
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

        # Data is already filtered by eligible processes (coddoc predicate pushdown)
        # No need for expensive join - data only contains eligible processes
        self.logger.info("✅ Data already filtered by eligible processes via predicate pushdown")
        valid_movement_codes = list(self.config_manager.get_situacao_codes("metas", "valid_movement_codes"))
        self.logger.info(f"All_numps: {all_mpums.columns}")
        if "cod_tip_mov_numero" in all_mpums.columns:
            eligible_mpums = all_mpums.filter(
                col("cod_tip_mov_numero").isin(valid_movement_codes)
            )
        else:
            eligible_mpums= all_mpums
        
        eligible_mpums = (eligible_mpums
                        .withColumn("row_number", 
                                    row_number().over(
                                        Window.partitionBy("coddoc")
                                            .orderBy("dthrmov")
                                    ))
                        .filter(col("row_number") == 1)
                        .drop("row_number"))
        
        # Add metadata
        result_df = eligible_mpums.withColumn("variavel", lit(f"mpum{self.meta_number}")) \
                                .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
                                .withColumn("dth_referencia", lit(self.period_end)) \
                                .withColumn("dth_calculo", current_timestamp()) \
                                .withColumn("periodo_inicio", lit(self.period_start)) \
                                .withColumn("periodo_fim", lit(self.period_end))
        
        # Optimize and cache
        result_df = result_df.repartition(400, "situacao").cache()
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        record_count = result_df.count()

        self.meta_logger.log_variable_completion(f"mpum{self.meta_number}", record_count, calculation_time)
        self.logger.info(f"✅ mpum calculation completed: {record_count:,} records in {calculation_time:.2f}s")

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
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
        
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
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end))
                    .withColumn("ano", year(col("dthrmov")))
                    .withColumn("mes", month(col("dthrmov")))
                    .withColumn("origin_data", lit("conhecimento")))
        
        result_df = result_df.repartition(400, "coddoc").cache()
        
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        record_count = result_df.count()
        self.meta_logger.log_variable_completion(f"julgadom{self.meta_number}", record_count, calculation_time)
        self.logger.info(f"✅ julgadom completed: {record_count:,} records in {calculation_time:.2f}s")
        
        return result_df

    def _calculate_susm_with_eligible_processes(self) -> DataFrame:
        """
        A soma dos processos do período enquadrados na variável "dis_k" 
        E não enquadrados na variável "julgados_k" com a situação Pendente (88) em aberto, 
        onde a situação Pendente líquido (25) esteja fechada no ano de 2025.
        """
        calculation_start = datetime.now()
        self.logger.info("🎯 Calculating susm (suspended processes) with complete Meta business rule...")
        
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
        )
        # Step 1: eligible_coddocs = cnm minus julgadom
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
        cnm_path = f"{_meta_export_path}/dism{self.meta_number}.parquet"
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}.parquet"
        sit_schema = StructType([
            StructField("coddoc", LongType(), True)
        ])
        # 1) Read cnm & julgadom with explicit schema and cast coddoc to long
        cnm_df = self.spark.read.schema(sit_schema).parquet(cnm_path).select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))
        self.logger.info(f"Distribuidos: {cnm_path}")
        julgadom_df = self.spark.read.schema(sit_schema).parquet(julgadom_path).select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))

        # 2) Compute eligible = cnm \ julgadom (DataFrame left_anti)
        eligible_coddocs_df = cnm_df.join(julgadom_df, on="coddoc", how="left_anti")

        # Quick guard: if eligible empty -> return empty result
        if eligible_coddocs_df.rdd.isEmpty():
            self.logger.info("No eligible processes -> returning empty result")
            return self.misc._create_empty_result_df()

        # 3) Load situacao 88 filtered by eligible (pass the DataFrame)
        situacao_88_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=88, coddoc_list=eligible_coddocs_df, apply_period_filter=True,
            period_start=self.meta_cutoff_date,
            period_end=self.period_end
            )

        # 4) Keep only latest open 88 per coddoc
        window_88 = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
        situacao_88_latest = (situacao_88_df
            .withColumn("_rn", row_number().over(window_88))
            .filter((col("_rn") == 1) & col("dth_fim").isNull())
            .select("coddoc")
            .distinct()
        )

        # 5) Load situacao 25 filtered by situacao_88_latest (DataFrame)
        situacao_25_df = self.load_situacao_with_dism_filter(
            situacao=25, apply_period_filter=True,
            period_start=date(2025,1,1),
            period_end=self.period_end
            )

        # 6) Keep latest closed 25 per coddoc
        window_25 = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
        # Keep full rows for the latest closed 25 per coddoc
        situacao_25_latest = (situacao_25_df
            .withColumn("_rn", row_number().over(window_25))
            .filter((col("_rn") == 1) & col("dth_fim").isNotNull())
            .drop("_rn")
        )

        # 7) Ensure the 25 rows are subset of those that had 88 (inner join on coddoc)
        # join with situacao_88_latest (which is a coddoc-only DF) but keep all columns from situacao_25_latest
        result_df_25_88 = situacao_25_latest.join(situacao_88_latest.select("coddoc"), "coddoc", "inner")

        # 8) Exclude julgadom (left_anti) using only coddoc column from julgadom_df
        result_df_clean = result_df_25_88.join(julgadom_df.select("coddoc"), "coddoc", "left_anti")

        
        # 9) Certify that will not have any cod_tip_mov = 239
        result_df_verify = result_df_clean.filter(trim(col("cod_tip_mov")) != 239)

        # 9) Add metadata columns to the full-row result DataFrame
        result_df = result_df_verify.withColumn("variavel", lit(f"susm{self.meta_number}")) \
            .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
            .withColumn("dth_referencia", lit(self.period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(self.period_start)) \
            .withColumn("periodo_fim", lit(self.period_end))
        result_df = result_df.repartition(400, "coddoc").cache()
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        record_count = result_df.count()
        self.meta_logger.log_variable_completion(f"susm{self.meta_number}", record_count, calculation_time)
        self.logger.info(f"✅ susm{self.meta_number} calculation completed: {record_count:,} records in {calculation_time:.2f}s")
        return result_df

    def _calculate_julgadom_ant(self) -> DataFrame:
        """
        Calculate julgadom_ant: judgement processes between a historical start (2000-01-01)
        and the configured cutoff date (self.meta_cutoff_date). Persist result to
        meta_export_path/current/julgadom{meta}_ant.parquet
        """
        from pyspark.sql.functions import col, lit, year, month, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        try:
            self.logger.info(f"🚀 Calculating julgadom{self.meta_number}_ant (historical judgments)...")
            _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
            dism_ant_path = f"{_meta_export_path}/dism{self.meta_number}_ant.parquet"
            dism_ant_df = self.spark.read.parquet(dism_ant_path).select('coddoc').distinct()
            judgment_situacoes = self.config_manager.get_situacao_codes("metas", "judgment_situacoes")
            julgado_parts = []

            for situacao in judgment_situacoes:
                situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                    situacao=situacao,
                    coddoc_list=dism_ant_df,
                    apply_period_filter=True,
                    period_start=self.meta_15year_cutoff,
                    period_end=self.period_end
                )
                if situacao_df is not None and not situacao_df.isEmpty():
                    julgado_parts.append(situacao_df)

            if julgado_parts:
                julgadom_df = julgado_parts[0]
                for df in julgado_parts[1:]:
                    julgadom_df = julgadom_df.unionByName(df, allowMissingColumns=True)
            else:
                self.logger.info("ℹ️ No historical judgment records found for window; producing empty DataFrame")
                return self.misc._create_empty_result_df()

            # Deduplicate at coddoc level (keep latest judgment movement)
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, desc
            w = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
            # Keep the full latest judgment row per coddoc (preserve all joined columns such as tab_xml_andamento)
            julgadom_latest = (
                julgadom_df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
            )

            # Preserve all columns from the latest judgment row
            result_df = (
                julgadom_latest
                .withColumn("variavel", lit(f"julgadom{self.meta_number}_ant"))
                .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                .withColumn("dth_referencia", lit(self.meta_cutoff_date))
                .withColumn("dth_calculo", current_timestamp())
            )

            result_df = result_df.repartition(200, "coddoc").cache()
            return result_df
        except Exception as e:
            self.logger.error(f"❌ Error in julgadom_ant calculation: {e}")
            return self.misc._create_empty_result_df()
        
    def _calculate_susm_ant(self) -> DataFrame:
        """
        Calculate susm_ant: processes with situacao 88 open and situacao 25 closed
        within the historical window (up to self.meta_15year_cutoff / cutoff). Uses
        existing helpers _calculate_old_88_pending and _calculate_old_25_close.
        Persists result to meta_export_path/current/susm{meta}_ant.parquet
        """
        self.logger.info("🎯 Calculating susm (suspended processes) with complete Meta business rule...")
        
        from pyspark.sql.functions import col, lit, year, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
        )
        calculation_start = datetime.now()
        # Step 1: eligible_coddocs = cnm minus julgadom
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
        cnm_path = f"{_meta_export_path}/dism{self.meta_number}_ant.parquet"
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}_ant.parquet"
        sit_schema = StructType([
            StructField("coddoc", LongType(), True)
        ])
        # 1) Read cnm & julgadom with explicit schema and cast coddoc to long
        cnm_df = self.spark.read.schema(sit_schema).parquet(cnm_path).select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))
        self.logger.info(f"Distribuidos: {cnm_path}")
        julgadom_df = self.spark.read.schema(sit_schema).parquet(julgadom_path).select("coddoc").distinct().withColumn("coddoc", col("coddoc").cast("long"))

        # 2) Compute eligible = cnm \ julgadom (DataFrame left_anti)
        eligible_coddocs_df = cnm_df.join(julgadom_df, on="coddoc", how="left_anti")

        # Quick guard: if eligible empty -> return empty result
        if eligible_coddocs_df.rdd.isEmpty():
            self.logger.info("No eligible processes -> returning empty result")
            return self.misc._create_empty_result_df()

        # 3) Load situacao 88 filtered by eligible (pass the DataFrame)
        situacao_88_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=88, coddoc_list=eligible_coddocs_df, apply_period_filter=True,
            period_start=date(2025,1,1),
            period_end=self.period_end
            )

        # 4) Keep only latest open 88 per coddoc
        window_88 = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
        situacao_88_latest = (situacao_88_df
            .withColumn("_rn", row_number().over(window_88))
            .filter((col("_rn") == 1) & col("dth_fim").isNull())
            .select("coddoc")
            .distinct()
        )

        # 5) Load situacao 25 filtered by situacao_88_latest (DataFrame)
        situacao_25_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=25, coddoc_list=situacao_88_latest, apply_period_filter=True,
            period_start=date(2025,1,1),
            period_end=self.period_end
            )

        # 6) Keep latest closed 25 per coddoc
        window_25 = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
        # Keep full rows for the latest closed 25 per coddoc
        situacao_25_latest = (situacao_25_df
            .withColumn("_rn", row_number().over(window_25))
            .filter((col("_rn") == 1) & col("dth_fim").isNotNull())
            .drop("_rn")
        )

        # 7) Ensure the 25 rows are subset of those that had 88 (inner join on coddoc)
        # join with situacao_88_latest (which is a coddoc-only DF) but keep all columns from situacao_25_latest
        result_df_25_88 = situacao_25_latest.join(situacao_88_latest.select("coddoc"), "coddoc", "inner")

        # 8) Exclude julgadom (left_anti) using only coddoc column from julgadom_df
        result_df_clean = result_df_25_88.join(julgadom_df.select("coddoc"), "coddoc", "left_anti")

        
        # 9) Certify that will not have any cod_tip_mov = 239
        result_df_verify = result_df_clean.filter(trim(col("cod_tip_mov")) != "239")

        # 9) Add metadata columns to the full-row result DataFrame
        result_df = result_df_verify.withColumn("variavel", lit(f"susm{self.meta_number}_ant")) \
            .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
            .withColumn("dth_referencia", lit(self.period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(self.period_start)) \
            .withColumn("periodo_fim", lit(self.period_end))
        result_df = result_df.repartition(400, "coddoc").cache()
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        record_count = result_df.count()
        self.meta_logger.log_variable_completion(f"susm{self.meta_number}", record_count, calculation_time)
        self.logger.info(f"✅ susm{self.meta_number} calculation completed: {record_count:,} records in {calculation_time:.2f}s")
        return result_df
 
    def _calculate_desm_ant_with_eligible_processes(self) -> DataFrame:
        from pyspark.sql.functions import col, lit, year, month, desc, asc, current_timestamp, row_number, regexp_replace, trim, when
        from pyspark.sql.window import Window
        calculation_start = datetime.now()
        self.logger.info(f"🎯 Calculating desm{self.meta_number} (unsuspended processes for Meta)...")
        # Step 0: Load 25 using all eligible processes by class filtering by meta period
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
        dism_ant_path = f"{_meta_export_path}/dism{self.meta_number}_ant.parquet"
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
        
        first_open_25_count = first_opened_25.count()
        opened_count = first_open_25_count
        self.logger.info(f"📊 Situacao 25 OPEN events in analysis month: {opened_count:,}")
        if opened_count == 0:
            return self.misc._create_empty_result_df()
        
        # Step 1: Remove 25 that are in julgadom
        julgadom_path = f"{_meta_export_path}/julgadom{self.meta_number}.parquet"

        julgadom_df = self.spark.read.parquet(julgadom_path).select("coddoc")
        opened_25_clean = first_opened_25.join(julgadom_df, "coddoc", "left_anti")
        self.logger.info(f"✅ Removed julgados with success")

        # Step 1.b: Remove julgado_ant
        julgadom_old_path = f"{_meta_export_path}/julgadom{self.meta_number}_ant.parquet"

        julgadom_old_df = self.spark.read.parquet(julgadom_old_path).select("coddoc")
        opened_25_clean = opened_25_clean.join(julgadom_old_df, "coddoc", "left_anti")
        self.logger.info(f"✅ Removed julgados with success")

        # Step 2: Confirm that the processes are pending
        pend_25_path = f"{_meta_export_path}/pend_25.parquet"
        pend_25_df = self.spark.read.parquet(pend_25_path).select("coddoc").distinct()
        opened_25_pending = opened_25_clean.join(pend_25_df, "coddoc", "inner")
        self.logger.info(f"✅ Processos that start pending")
        
        # Step 3: Verify status of the register: 88 
        situacao_88_path = f"{_meta_export_path}/pend_88_old.parquet"
        situacao_88_df = self.spark.read.parquet(situacao_88_path).select("coddoc").distinct()
        opened_25_pending = opened_25_pending.join(situacao_88_df, "coddoc", "inner")
        self.logger.info(f"✅ 88 Pending and 25 Closed")
        # Step 4: Certify that our processes match the result of the Step 3
        final_df = opened_25_pending
        self.logger.info(f"✅ Keep just the processes that match all criteria")
        # Add metadata columns and return
        result_df = (final_df
                    .withColumn("variavel", lit(f"desm{self.meta_number}"))
                    .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                    .withColumn("dth_referencia", lit(self.period_end))
                    .withColumn("dth_calculo", current_timestamp())
                    .withColumn("periodo_inicio", lit(self.period_start))
                    .withColumn("periodo_fim", lit(self.period_end))
                    .withColumn("ano", year(col("dthrmov")))
                    .withColumn("mes", month(col("dthrmov")))
                    .withColumn("origin_data", lit(f"desm{self.meta_number}_unsuspension"))
                    )

        result_df = result_df.repartition(400, "coddoc").cache()
        calculation_time = (datetime.now() - calculation_start).total_seconds()
        record_count = result_df.count()
        self.meta_logger.log_variable_completion(f"desm{self.meta_number}", record_count, calculation_time)
        self.logger.info(f"✅ des{self.meta_number} calculation completed: {record_count:,} records in {calculation_time:.2f}s")

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
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
        path = f"{_meta_export_path}/dism{self.meta_number}.parquet"
        dism_processes_df = self.spark.read.parquet(path)
        dism_processes_df = dism_processes_df.withColumn("coddoc", col("coddoc").cast("long"))
        self.logger.info(f"Eligible processes count: {dism_processes_df.count()}")
        # Check if dism processes DataFrame is empty
        try:
            dism_processes_list = dism_processes_df.select("coddoc").distinct()
            dism_count = dism_processes_list.count()
            if dism_count == 0:
                self.logger.warning(f"No dism processes available (DataFrame is empty) - returning empty DataFrame")
                return self.misc._create_empty_result_df()
            else:
                self.logger.info(f"📊 Sending {dism_count:,} dism processes to hybrid loader for situacao {situacao}")
                
        except Exception as e:
            self.logger.error(f"Error checking dism processes count: {e}")
            return None
            
        try:            
            df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=dism_processes_list, 
                apply_period_filter=True,
                period_start=period_start,
                period_end=period_end
            )
            self.logger.info(f"df_loaded columns: {df.columns}")
            if df is not None:
                self.logger.info(f"✅ Successfully loaded situacao {situacao} with predicate pushdown: {df.count():,} records")
                return df
            else:
                self.logger.warning(f"⚠️ Predicate pushdown returned no data for situacao {situacao}")
                return self.misc._create_empty_result_df()
                
        except Exception as e:
            self.logger.error(f"❌ Failed to load situacao {situacao} with predicate pushdown: {e}")