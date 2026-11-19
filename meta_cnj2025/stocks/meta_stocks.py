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
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, count, sum as spark_sum,
        max as spark_max, min as spark_min, first, desc, asc,
        year, month, dayofmonth, to_date, date_format,
        broadcast, coalesce, current_timestamp, concat_ws, 
        row_number, rank, dense_rank
    )
    from pyspark.sql.window import Window
    from pyspark import StorageLevel
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

class MetaCalculator:
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
        self.meta_15year_cutoff = meta_15year_cutoff
        self.meta_number = meta_number
        
        # Initialize optimized stock table paths
        self._init_stock_table_paths()

    def _parquet_has_data(self, path: str) -> bool:
        """Check whether a parquet directory or file contains any data files."""
        try:
            if not path or not os.path.exists(path):
                return False

            if os.path.isfile(path):
                return os.path.getsize(path) > 0

            for root, _, files in os.walk(path):
                for fname in files:
                    if fname.startswith("_"):
                        continue
                    full_path = os.path.join(root, fname)
                    try:
                        if os.path.getsize(full_path) > 0:
                            return True
                    except OSError:
                        continue
            return False
        except Exception as e:
            self.logger.debug(f"DEBUG: Failed parquet inspection at {path}: {e}")
            return False

    def _init_stock_table_paths(self):
        """Initialize pre-computed stock table paths for optimized calculations."""
        import os
        
        stock_base_path = "/opt/parquet/data/prata/ejud"
        
        self.tab_distribuicao_path = os.path.join(stock_base_path, "tab_distribuicao")
        self.tab_julgamento_path = os.path.join(stock_base_path, "tab_julgamento") 
        self.judmigracao_table_path = os.path.join(stock_base_path, "tab_judmigracao")
        
        self.stock_tables_available = {
            'tab_distribuicao': self._parquet_has_data(self.tab_distribuicao_path),
            'tab_julgamento': self._parquet_has_data(self.tab_julgamento_path),
            'judmigracao_table': self._parquet_has_data(self.judmigracao_table_path)
        }

    def _read_stock_table(self, path: str) -> "DataFrame":
        """Read a partitioned stock table.

        Assumes all partitions have consistent physical types for cod_tip_mov.
        If mixed types exist, the old partitions must be regenerated first.
        """
        df = self.spark.read.parquet(path)
        if "cod_tip_mov" in df.columns:
            df = df.withColumn("cod_tip_mov", col("cod_tip_mov").cast("int"))
        return df

    def _build_inline_distribuicao(self, coddoc_df: "DataFrame", cutoff_date) -> "DataFrame":
        """
        Inline tab_distribuicao: earliest dthrmov per coddoc from
        situacao [9, 65, 81, 61], row_number() OVER (PARTITION BY coddoc ORDER BY dthrmov ASC) == 1.
        """
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window
        from datetime import date as _date

        DIST_SITUACOES = [9, 65, 81, 61]
        frames = []
        for sit in DIST_SITUACOES:
            df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=sit,
                coddoc_list=coddoc_df,
                apply_period_filter=True,
                period_start=_date(1980, 1, 1),
                period_end=cutoff_date,
            )
            if df is not None:
                try:
                    if not df.isEmpty():
                        frames.append(df)
                except Exception:
                    frames.append(df)

        if not frames:
            self.logger.warning("_build_inline_distribuicao: no distribuicao records found")
            return self.misc._create_empty_result_df()

        union_df = frames[0]
        for f in frames[1:]:
            union_df = union_df.unionByName(f, allowMissingColumns=True)

        w = Window.partitionBy("coddoc").orderBy(col("dthrmov").asc())
        return (
            union_df
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

    def _build_inline_julgamento(self, coddoc_df: "DataFrame", cutoff_date) -> "DataFrame":
        """
        Inline tab_julgamento: latest dthrmov per coddoc from
        situacao [62,27,28,2,10,23,41,18,29,90,129,26,65,91,134,154,132,17,34].
        sit-17 guard: only keep sit-17 rows when cod_tip_mov IN
        [11423,11424,11425,12476,12479,15486,15487,15488].
        row_number() OVER (PARTITION BY coddoc ORDER BY dthrmov DESC) == 1.
        """
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window
        from datetime import date as _date

        JULG_SITUACOES = [62, 27, 28, 2, 10, 23, 41, 18, 29, 90, 129, 26, 65, 91, 134, 154, 132, 17, 34]
        SIT17_VALID_COD_TIP_MOV = [11423, 11424, 11425, 12476, 12479, 15486, 15487, 15488]
        frames = []
        for sit in JULG_SITUACOES:
            df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=sit,
                coddoc_list=coddoc_df,
                apply_period_filter=True,
                period_start=_date(1980, 1, 1),
                period_end=cutoff_date,
            )
            if df is not None:
                try:
                    if not df.isEmpty():
                        frames.append(df)
                except Exception:
                    frames.append(df)

        if not frames:
            self.logger.warning("_build_inline_julgamento: no julgamento records found")
            return self.misc._create_empty_result_df()

        union_df = frames[0]
        for f in frames[1:]:
            union_df = union_df.unionByName(f, allowMissingColumns=True)

        # Apply sit-17 guard
        if "cod_tip_mov" in union_df.columns:
            union_df = union_df.withColumn("cod_tip_mov", col("cod_tip_mov").cast("int"))
            union_df = union_df.filter(
                (col("situacao").cast("int") != 17) |
                col("cod_tip_mov").isin(SIT17_VALID_COD_TIP_MOV)
            )

        w = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())
        return (
            union_df
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

    def _calculate_dism_optimized_with_stock_tables(self) -> DataFrame:
        """
        Optimized dism calculation using inline-computed distribuicao/julgamento.
        tab_distribuicao and tab_julgamento are derived on-the-fly from source
        situacao data so the calculation is not blocked by missing parquet partitions.
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number
        from pyspark.sql.window import Window

        self.logger.debug("🚀 Calculating dism (inline)...")

        try:
            pending_situacao = 88
            pendente_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=pending_situacao,
                coddoc_list=self.eligible_processes_df,
                apply_period_filter=True,
                period_start=date(1970, 1, 1),
                period_end=self.meta_cutoff_date,
            )

            if pendente_df is None or pendente_df.isEmpty():
                return self.misc._create_empty_result_df()

            # Build inline distribuicao (earliest distribution event per coddoc)
            distribuicao_df = self._build_inline_distribuicao(
                self.eligible_processes_df, self.meta_cutoff_date
            )

            if distribuicao_df is None or distribuicao_df.isEmpty():
                return self._calculate_dism_with_eligible_processes()

            # Build inline julgamento (latest judgment event per coddoc),
            # constrained to processes that were already distributed.
            julgamento_df = self._build_inline_julgamento(
                distribuicao_df, self.meta_cutoff_date
            )

            result_df = distribuicao_df.join(
                julgamento_df.select("coddoc").distinct(),
                on="coddoc",
                how="left_anti",
            )

            if self._parquet_has_data(self.judmigracao_table_path):
                judmigracao_df = self.spark.read.parquet(self.judmigracao_table_path)
                result_df = result_df.join(
                    judmigracao_df.select("coddoc").distinct(),
                    on="coddoc",
                    how="left_anti",
                )

            # Gate: keep only coddocs that have a sit-88 event before cutoff.
            final_df = pendente_df.join(
                result_df.select("coddoc").distinct(),
                on="coddoc",
                how="inner",
            )

            window = Window.partitionBy("coddoc").orderBy("dthrmov")
            final_df = (
                final_df
                .withColumn("rn", row_number().over(window))
                .filter(col("rn") == 1)
                .drop("rn")
            )

            final_df = (
                final_df
                .withColumn("variavel", lit(f"dism{self.meta_number}"))
                .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                .withColumn("dth_referencia", lit(self.period_end))
                .withColumn("dth_calculo", current_timestamp())
                .withColumn("periodo_inicio", lit(self.period_start))
                .withColumn("periodo_fim", lit(self.period_end))
            )

            self.logger.debug("✅ dism persisted (partitioned by coddoc)")
            return final_df

        except Exception as e:
            self.logger.error(f"❌ Error in dism calculation: {e}")
            return self._calculate_dism_with_eligible_processes()

    def _calculate_dism_old_optimized_with_stock_tables(self) -> DataFrame:
        """
        Optimized dism_old calculation using inline-computed distribuicao/julgamento.
        tab_distribuicao and tab_julgamento are derived on-the-fly from source
        situacao data so the calculation is not blocked by missing parquet partitions.
        """
        from pyspark.sql.functions import col, lit, current_timestamp, row_number
        from pyspark.sql.window import Window

        self.logger.debug("🚀 Calculating dism_old (inline)...")

        try:
            # Build inline distribuicao (earliest distribution event per coddoc)
            distribuicao_df = self._build_inline_distribuicao(
                self.eligible_processes_df, self.meta_15year_cutoff
            )

            if distribuicao_df is None or distribuicao_df.isEmpty():
                return self._calculate_dism_old_with_eligible_processes()

            # Build inline julgamento (latest judgment event per coddoc),
            # constrained to processes that were already distributed.
            julgamento_df = self._build_inline_julgamento(
                distribuicao_df, self.meta_cutoff_date
            )

            result_df = distribuicao_df.join(
                julgamento_df.select("coddoc").distinct(),
                on="coddoc",
                how="left_anti",
            )

            if self._parquet_has_data(self.judmigracao_table_path):
                judmigracao_df = self.spark.read.parquet(self.judmigracao_table_path)
                result_df = result_df.join(
                    judmigracao_df.select("coddoc").distinct(),
                    on="coddoc",
                    how="left_anti",
                )

            final_df = result_df
            window = Window.partitionBy("coddoc").orderBy("dthrmov")
            final_df = (
                final_df
                .withColumn("rn", row_number().over(window))
                .filter(col("rn") == 1)
                .drop("rn")
            )

            final_df = (
                final_df
                .withColumn("variavel", lit(f"dism{self.meta_number}_ant"))
                .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                .withColumn("dth_referencia", lit(self.period_end))
                .withColumn("dth_calculo", current_timestamp())
                .withColumn("periodo_inicio", lit(self.period_start))
                .withColumn("periodo_fim", lit(self.period_end))
            )

            self.logger.debug("✅ dism_old persisted (partitioned by coddoc)")
            return final_df

        except Exception as e:
            self.logger.error(f"❌ Error in dism_old calculation: {e}")
            return self._calculate_dism_old_with_eligible_processes()


    def _calculate_dism_with_eligible_processes(self) -> DataFrame:
        """Fallback dism calculation using situacao loaders when stock tables are unavailable."""
        from pyspark.sql.functions import lit, current_timestamp, col, row_number
        from pyspark.sql.window import Window

        self.logger.debug("🔁 Fallback dism calculation...")

        try:
            if self.eligible_processes_df is None:
                return self.misc._create_empty_result_df()

            try:
                if self.eligible_processes_df.isEmpty():
                    return self.misc._create_empty_result_df()
            except Exception:
                pass

            cutoff_date = self.meta_cutoff_date or self.period_end
            distribution_start = date(1980, 1, 1)

            pending_situacao = 88
            pendente_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=pending_situacao,
                coddoc_list=self.eligible_processes_df,
                apply_period_filter=True,
                period_start=date(1970, 1, 1),
                period_end=cutoff_date
            )

            if pendente_df is None or pendente_df.isEmpty():
                return self.misc._create_empty_result_df()

            distribution_situacoes = self.config_manager.get_situacao_codes("metas", "distribution_situacoes")
            distr_frames = []
            for situacao in distribution_situacoes:
                situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                    situacao=situacao,
                    coddoc_list=self.eligible_processes_df,
                    apply_period_filter=True,
                    period_start=distribution_start,
                    period_end=cutoff_date
                )
                if situacao_df is not None and not situacao_df.isEmpty():
                    distr_frames.append(situacao_df)

            if not distr_frames:
                return self.misc._create_empty_result_df()

            distribuicao_df = distr_frames[0]
            for df in distr_frames[1:]:
                distribuicao_df = distribuicao_df.unionByName(df, allowMissingColumns=True)

            #distribuicao_df = self.misc._persist_dataframe_safely(distribuicao_df)

            julgamento_situacoes = self.config_manager.get_situacao_codes("metas", "judgment_situacoes")
            julgamento_frames = []
            for situacao in julgamento_situacoes:
                situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                    situacao=situacao,
                    coddoc_list=distribuicao_df,
                    apply_period_filter=True,
                    period_start=distribution_start,
                    period_end=cutoff_date
                )
                if situacao_df is not None and not situacao_df.isEmpty():
                    julgamento_frames.append(situacao_df)

            if julgamento_frames:
                julgamento_df = julgamento_frames[0]
                for df in julgamento_frames[1:]:
                    julgamento_df = julgamento_df.unionByName(df, allowMissingColumns=True)
            else:
                julgamento_df = self.misc._create_empty_result_df()

            result_df = distribuicao_df.join(
                julgamento_df.select("coddoc").distinct(),
                on="coddoc",
                how="left_anti"
            )

            if self._parquet_has_data(self.judmigracao_table_path):
                judmigracao_df = self.spark.read.parquet(self.judmigracao_table_path).select("coddoc").distinct()
                result_df = result_df.join(judmigracao_df, "coddoc", "left_anti")

            final_df = pendente_df.join(
                result_df.select("coddoc").distinct(),
                on="coddoc",
                how="inner"
            )

            window = Window.partitionBy("coddoc").orderBy(col("dthrmov"))
            final_df = (final_df
                .withColumn("rn", row_number().over(window))
                .filter(col("rn") == 1)
                .drop("rn"))

            final_df = (final_df
                .withColumn("variavel", lit(f"dism{self.meta_number}"))
                .withColumn("meta_indicator", lit(f"meta_{self.meta_number}"))
                .withColumn("dth_referencia", lit(self.period_end))
                .withColumn("dth_calculo", current_timestamp())
                .withColumn("periodo_inicio", lit(self.period_start))
                .withColumn("periodo_fim", lit(self.period_end))
            )

            #final_df = self.misc._persist_dataframe_safely(final_df, partition_by="coddoc")
            self.logger.debug("✅ Fallback dism persisted (partitioned by coddoc)")
            return final_df

        except Exception as e:
            self.logger.error(f"❌ Fallback dism failed: {e}")
            return self.misc._create_empty_result_df()


    def _calculate_old_88_pending(self) -> DataFrame:
        """Calculate pending situations for code 88."""
        from pyspark.sql.functions import col, lit, desc, current_timestamp, row_number
        from pyspark.sql.window import Window
        
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
        filtered_eligible_processes = self.eligible_processes_df 
            
        dism_ant_df = self.spark.read.parquet(f"{_meta_export_path}/dism{self.meta_number}_ant.parquet").select('coddoc').distinct()
        sit_88_pending_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=88,
            coddoc_list=filtered_eligible_processes,
            apply_period_filter=True,
            period_start=date(1970,1,1),
            period_end=self.period_start
        )

        window_88 = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))
        
        sit_88_latest = (
            sit_88_pending_df
            .withColumn("_rn", row_number().over(window_88))
            .filter((col("_rn") == 1) & (col("dth_fim").isNull()))
            .drop("_rn")
        )
        
        result_df = sit_88_latest.withColumn("variavel", lit("pend_old_88")) \
            .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
            .withColumn("dth_referencia", lit(self.period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(self.period_start)) \
            .withColumn("periodo_fim", lit(self.period_end))
        #result_df = self.misc._persist_dataframe_safely(result_df, partition_by="coddoc")
        self.logger.debug("✅ pend_old_88 persisted (partitioned by coddoc)")
        return result_df

    def _calculate_old_25_close(self) -> DataFrame:
        """Calculate pending situations for code 25."""
        from pyspark.sql.functions import col, lit, desc, current_timestamp, row_number
        from pyspark.sql.window import Window
        
        _meta_export_path = meta_export_path(f"meta_{self.meta_number}", "ejud")
        filtered_eligible_processes = self.eligible_processes_df 
        dism_ant_df = self.spark.read.parquet(f"{_meta_export_path}/dism{self.meta_number}_ant.parquet").select('coddoc').distinct()
        
        sit_25_pending_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
            situacao=25,
            coddoc_list=filtered_eligible_processes,
            apply_period_filter=True,
            period_start=date(1970,1,1),
            period_end=self.period_start
        )

        window_25 = Window.partitionBy("coddoc").orderBy(desc("dthrmov"))

        sit_25_latest = (
            sit_25_pending_df
            .withColumn("_rn", row_number().over(window_25))
            .filter((col("_rn") == 1) & (col("dth_fim").isNotNull()))
            .drop("_rn")
        )
        
        result_df = sit_25_latest.withColumn("variavel", lit("pend_old_25")) \
            .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
            .withColumn("dth_referencia", lit(self.period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(self.period_start)) \
            .withColumn("periodo_fim", lit(self.period_end))
        #result_df = self.misc._persist_dataframe_safely(result_df, partition_by="coddoc")
        self.logger.debug("✅ pend_old_25 persisted (partitioned by coddoc)")
        return result_df

    def _calculate_dism_old_with_eligible_processes(self) -> DataFrame:
        """Calculate history of processes with distribution and without judgment."""
        from pyspark.sql.functions import lit, current_timestamp, col
        
        distribution_situacoes = self.config_manager.get_situacao_codes("metas", "distribution_situacoes")
        
        distr_df = []
        period_start = date(1980, 1, 1)
        period_end = self.meta_15year_cutoff
        
        for situacao in distribution_situacoes:
            situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=self.eligible_processes_df,
                apply_period_filter=True,
                period_start=period_start,
                period_end=period_end
            )
            if situacao_df is not None and not situacao_df.isEmpty():
                distr_df.append(situacao_df)
        
        if distr_df:
            result_distr_df = distr_df[0]
            for df in distr_df[1:]:
                result_distr_df = result_distr_df.unionByName(df, allowMissingColumns=True)
        else:
            result_distr_df = self.misc._create_empty_result_df()

        julgado_situacoes = self.config_manager.get_situacao_codes("metas", "judgment_situacoes")
        julgado_df = []
        
        for situacao in julgado_situacoes:
            situacao_df = self.hybrid_loader.load_situacao_with_coddoc_filter(
                situacao=situacao,
                coddoc_list=result_distr_df,
                apply_period_filter=True,
                period_start=date(1980,1,1),
                period_end=date(2024,12,31)
            )
            if situacao_df is not None and not situacao_df.isEmpty():
                julgado_df.append(situacao_df)
        
        if julgado_df:
            result_julgado_df = julgado_df[0]
            for df in julgado_df[1:]:
                result_julgado_df = result_julgado_df.unionByName(df, allowMissingColumns=True)
        else:
            result_julgado_df = self.misc._create_empty_result_df()

        distrib_pending_judgment = result_distr_df.join(result_julgado_df, "coddoc", "left_anti")
        stock_base_path = "/opt/parquet/data/prata/ejud"
        self.judmigracao_table_path = os.path.join(stock_base_path, "tab_judmigracao")

        if self._parquet_has_data(self.judmigracao_table_path):
            judmigracao_df = self.spark.read.parquet(self.judmigracao_table_path).select("coddoc")
            distrib_pending_judgment = distrib_pending_judgment.join(judmigracao_df, "coddoc", "left_anti")

        distrib_pending_judgment = distrib_pending_judgment.withColumn("variavel", lit(f"dism{self.meta_number}_ant")) \
            .withColumn("meta_indicator", lit(f"meta_{self.meta_number}")) \
            .withColumn("dth_referencia", lit(period_end)) \
            .withColumn("dth_calculo", current_timestamp()) \
            .withColumn("periodo_inicio", lit(period_start)) \
            .withColumn("periodo_fim", lit(period_end))
        result_df = distrib_pending_judgment
        #result_df = self.misc._persist_dataframe_safely(distrib_pending_judgment, partition_by="coddoc")
        self.logger.debug("✅ dism_old (fallback) persisted (partitioned by coddoc)")
        return result_df
    
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

    def _load_meta04_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 04 eligible processes with case type differentiation...")
        self.logger.info(f"🔍 Debug - meta04_filter_paths: {self.meta04_filter_paths}")
        self.logger.info(f"🔍 Debug - meta04_filter_paths type: {type(self.meta04_filter_paths)}")
        
        if not self.meta04_filter_paths:
            self.logger.error("❌ No meta04_filter_paths configured!")
            return None
            
        filter_data = {}
                
        for filter_path in self.meta04_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # The config manager already returns absolute paths, use them directly
                self.logger.info(f"   🔍 Using absolute path from config: {filter_path}")
                
                # Check if file exists
                if not Path(filter_path).exists():
                    self.logger.error(f"   ❌ Filter file does not exist: {filter_path}")
                    continue
                    
                # Use Spark's native parquet reader for filter files
                df = self.spark.read.parquet(filter_path)
                if df is not None:
                    # Identify case type based on filename
                    if "conhecimento" in filter_name:
                        filter_data["conhecimento"] = df.select("coddoc")
                    elif "improbidade" in filter_name:
                        filter_data["improbidade"] = df.select("coddoc")
                        self.logger.info(f"   ⚖️ Improbidade Administrativa cases")
                    elif "adm_publica" in filter_name:
                        filter_data["adm_publica"] = df.select("coddoc")
                        self.logger.info(f"   🏛️ Crimes contra Admin Pública cases")
                        
            except Exception as e:
                self.logger.error(f"❌ Error loading {filter_path}: {e}")
                continue
        
        # Apply intersection logic: CONHECIMENTO AND (adm_publica OR improbidade)
        if "conhecimento" not in filter_data:
            self.logger.error("❌ Missing required conhecimento filter")
            return None
            
        if "adm_publica" not in filter_data and "improbidade" not in filter_data:
            self.logger.error("❌ Missing both adm_publica and improbidade filters")
            return None
        
        conhecimento_processes = filter_data["conhecimento"]
        
        # Union adm_publica and improbidade
        if "adm_publica" in filter_data and "improbidade" in filter_data:
            public_processes = filter_data["adm_publica"].unionByName(filter_data["improbidade"], allowMissingColumns=True)
        elif "adm_publica" in filter_data:
            public_processes = filter_data["adm_publica"]
        else:
            public_processes = filter_data["improbidade"]
        
        # Final intersection: conhecimento AND (adm_publica OR improbidade)
        eligible_processes = (conhecimento_processes
                             .join(public_processes, on="coddoc", how="inner")
                             .select("coddoc").distinct())

        # Store and filter individual case type filters so they are constrained to the
        # final eligible_processes intersection. Also ensure unique coddoc for each.
        raw_crimes_admin = filter_data.get("adm_publica")
        raw_improbity = filter_data.get("improbidade")

        # Helper: if a raw filter is present, inner join with eligible_processes and deduplicate.
        if raw_crimes_admin is not None:
            crimes_admin_filtered = (raw_crimes_admin.join(eligible_processes, on="coddoc", how="inner")
                                      .select("coddoc").distinct())
        else:
            crimes_admin_filtered = self.misc._create_empty_result_df()

        if raw_improbity is not None:
            improbity_filtered = (raw_improbity.join(eligible_processes, on="coddoc", how="inner")
                                   .select("coddoc").distinct())
        else:
            improbity_filtered = self.misc._create_empty_result_df()

        # Store filtered DataFrames on the instance for later use
        self.crimes_admin_processes = crimes_admin_filtered
        self.improbity_processes = improbity_filtered

        # Persist safely (this forces evaluation and returns a persisted DataFrame)
        eligible_processes_adm = self.misc._persist_dataframe_safely(self.crimes_admin_processes)
        eligible_processes_improbity = self.misc._persist_dataframe_safely(self.improbity_processes)

        return eligible_processes_adm, eligible_processes_improbity

    def _load_meta06_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 06 eligible processes (environmental crimes)...")
        filter_data = {}
                
        for filter_path in self.meta06_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # Use Spark's native parquet reader for filter files
                df = self.spark.read.parquet(filter_path)
                if df is not None:
                    self.logger.info(f"   ✅ Loaded processes from {filter_name}")
                    
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
        
        self.logger.info(f"✅ Meta 06 eligible processes (environmental crimes) loaded")
        
        return eligible_processes

    def _load_meta08_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 04 eligible processes with case type differentiation...")
        self.logger.info(f"🔍 Debug - meta_filter_paths: {self.meta_filter_paths}")
        self.logger.info(f"🔍 Debug - meta_filter_paths type: {type(self.meta_filter_paths)}")
        
        if not self.meta_filter_paths:
            self.logger.error("❌ No meta_filter_paths configured!")
            return None
            
        filter_data = {}
                
        for filter_path in self.meta_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # The config manager already returns absolute paths, use them directly
                self.logger.info(f"   🔍 Using absolute path from config: {filter_path}")
                
                # Check if file exists
                if not Path(filter_path).exists():
                    self.logger.error(f"   ❌ Filter file does not exist: {filter_path}")
                    continue
                    
                # Use Spark's native parquet reader for filter files
                df = self.spark.read.parquet(filter_path)
                if df is not None:
                    self.logger.info(f"   ✅ Loaded processes from {filter_name}")
                    
                    # Identify case type based on filename
                    if "conhecimento" in filter_name:
                        filter_data["conhecimento"] = df.select("coddoc")
                    elif "feminicidio" in filter_name:
                        filter_data["feminicidio"] = df.select("coddoc")
                        self.logger.info(f"   ⚖️ feminicidio Administrativa cases")
                    elif "violencia_domestica" in filter_name:
                        filter_data["violencia_domestica"] = df.select("coddoc")
                        self.logger.info(f"   🏛️ Crimes contra Admin Pública cases")
                        
            except Exception as e:
                self.logger.error(f"❌ Error loading {filter_path}: {e}")
                continue
        
        # Apply intersection logic: CONHECIMENTO AND (violencia_domestica OR feminicidio)
        if "conhecimento" not in filter_data:
            self.logger.error("❌ Missing required conhecimento filter")
            return None
            
        if "violencia_domestica" not in filter_data and "feminicidio" not in filter_data:
            self.logger.error("❌ Missing both violencia_domestica and feminicidio filters")
            return None
        
        conhecimento_processes = filter_data["conhecimento"]
        
        # Union violencia_domestica and feminicidio
        if "violencia_domestica" in filter_data and "feminicidio" in filter_data:
            public_processes = filter_data["violencia_domestica"].unionByName(filter_data["feminicidio"], allowMissingColumns=True)
        elif "violencia_domestica" in filter_data:
            public_processes = filter_data["violencia_domestica"]
        else:
            public_processes = filter_data["feminicidio"]
        
        # Final intersection: conhecimento AND (violencia_domestica OR feminicidio)
        eligible_processes = (conhecimento_processes
                             .join(public_processes, on="coddoc", how="inner")
                             .select("coddoc").distinct())

        # Store and filter individual case type filters so they are constrained to the
        # final eligible_processes intersection. Also ensure unique coddoc for each.
        raw_viol_domestica = filter_data.get("violencia_domestica")
        raw_feminicidio = filter_data.get("feminicidio")

        # Helper: if a raw filter is present, inner join with eligible_processes and deduplicate.
        if raw_viol_domestica is not None:
            crimes_viol_domestica_filtered = (raw_viol_domestica.join(eligible_processes, on="coddoc", how="inner")
                                      .select("coddoc").distinct())
        else:
            crimes_viol_domestica_filtered = self.misc._create_empty_result_df()

        if raw_feminicidio is not None:
            feminicidio_filtered = (raw_feminicidio.join(eligible_processes, on="coddoc", how="inner")
                                   .select("coddoc").distinct())
        else:
            feminicidio_filtered = self.misc._create_empty_result_df()

        # Store filtered DataFrames on the instance for later use
        crimes_viol_domestica_processes = crimes_viol_domestica_filtered
        feminicidio_processes = feminicidio_filtered

        excluded_processes = self.get_user_exclusion_filter()
        if crimes_viol_domestica_processes is not None:
            crimes_viol_domestica_processes = crimes_viol_domestica_processes.join(excluded_processes, on="coddoc", how="left_anti")
        if feminicidio_processes is not None:
            feminicidio_processes = feminicidio_processes.join(excluded_processes, on="coddoc", how="left_anti")
        # Persist safely (this forces evaluation and returns a persisted DataFrame)
        eligible_processes_adm = self.misc._persist_dataframe_safely(crimes_viol_domestica_processes)
        eligible_processes_feminicidio = self.misc._persist_dataframe_safely(feminicidio_processes)
        
        # Store case type information for use in exports (for priority handling)
        self.feminicidio_coddocs = eligible_processes_feminicidio
        self.violencia_domestica_coddocs = eligible_processes_adm
        
        eligible_processes = eligible_processes_adm.union(eligible_processes_feminicidio).distinct()
        
        return eligible_processes

    def _load_meta10_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 10 eligible processes using intersection logic...")
        filter_data = {}
                
        for filter_path in self.meta10_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # Use glob pattern helper to avoid reading metadata files
                df = self.spark.read.parquet(filter_path)
                if df is not None:
                    self.logger.info(f"   ✅ Loaded processes from {filter_name}")
                    # Capture known filter types
                    if "conhecimento" in filter_name:
                        filter_data["conhecimento"] = df.select("coddoc")
                    if "crianca_adolescente" in filter_name:
                        filter_data["crianca_adolescente"] = df.select("coddoc")
                        
            except Exception as e:
                self.logger.error(f"❌ Error loading {filter_path}: {e}")
                continue
        
           
        # Require both conhecimento and crianca_adolescente to perform intersection
        # If conhecimento is missing but crianca_adolescente exists, fall back to using crianca as base
        if "conhecimento" not in filter_data:
            if "crianca_adolescente" in filter_data:
                self.logger.warning("⚠️ conhecimento filter missing - falling back to crianca_adolescente as eligible set (first-run fallback)")
                filter_data["conhecimento"] = filter_data["crianca_adolescente"]
            else:
                self.logger.error("❌ Missing required conhecimento filter")
                return None
        if "crianca_adolescente" not in filter_data:
            self.logger.error("❌ Missing required crianca_adolescente filter")
            return None

        conhecimento_processes = filter_data["conhecimento"]
        crianca_processes = filter_data["crianca_adolescente"]

        # Final intersection
        eligible_processes = (conhecimento_processes.join(crianca_processes, on="coddoc", how="inner").select("coddoc").distinct())

        excluded_processes = self.get_user_exclusion_filter()
        if eligible_processes is not None:
            eligible_processes = eligible_processes.join(excluded_processes, on="coddoc", how="left_anti")
        
        # Persist safely
        eligible_processes = self.misc._persist_dataframe_safely(eligible_processes)
        
        self.logger.info(f"✅ Meta 10 eligible processes (from intersection)")
        
        return eligible_processes

    def _load_meta11_eligible_processes(self) -> DataFrame:
        self.logger.info("🔄 Loading Meta 11 eligible processes using intersection logic...")
        filter_data = {}
                
        for filter_path in self.meta11_filter_paths:
            try:
                filter_name = Path(filter_path).name
                self.logger.info(f"📂 Loading filter: {filter_name}")
                
                # Use glob pattern helper to avoid reading metadata files
                df = self.spark.read.parquet(filter_path)
                if df is not None:
                    # Capture known filter types
                    if "conhecimento" in filter_name:
                        filter_data["conhecimento"] = df.select("coddoc")
                    if "idosos" in filter_name:
                        filter_data["idosos"] = df.select("coddoc")
                        
            except Exception as e:
                self.logger.error(f"❌ Error loading {filter_path}: {e}")
                continue
        
           
        # Require both conhecimento and idosos to perform intersection
        # If conhecimento is missing but idosos exists, fall back to using crianca as base
        if "conhecimento" not in filter_data:
            if "idosos" in filter_data:
                self.logger.warning("⚠️ conhecimento filter missing - falling back to idosos as eligible set (first-run fallback)")
                filter_data["conhecimento"] = filter_data["idosos"]
            else:
                self.logger.error("❌ Missing required conhecimento filter")
                return None
        if "idosos" not in filter_data:
            self.logger.error("❌ Missing required idosos filter")
            return None

        conhecimento_processes = filter_data["conhecimento"]
        crianca_processes = filter_data["idosos"]

        # Final intersection
        eligible_processes = (conhecimento_processes.join(crianca_processes, on="coddoc", how="inner").select("coddoc").distinct())

        excluded_processes = self.get_user_exclusion_filter()
        if eligible_processes is not None:
            eligible_processes = eligible_processes.join(excluded_processes, on="coddoc", how="left_anti")
        
        
        # Persist safely
        eligible_processes = self.misc._persist_dataframe_safely(eligible_processes)
        
        return eligible_processes
        
    
    def get_user_exclusion_filter(self, movimento_base_path: str = None) -> DataFrame:
        """
        Identifies processes where the latest movement was made by excluded users 
        (EJUDAPURACAO, JUDIMIGRACAO) indicating test/migration data.
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
            return excluded_processes
            
        except Exception as e:
            self.logger.error(f"❌ Error creating user exclusion filter: {e}")
            return self.misc._create_empty_result_df()
