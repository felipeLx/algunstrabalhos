import os, glob, time, json, sys
from pathlib import Path
from typing import Dict, List
import gc
from datetime import date, datetime
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from utils.debug_utils import is_debug, debug_log_count
from utils.save_to_postgres import save_df_to_postgres_no_jar
from utils.parquet_utils import get_bronze_path, parquet_path, bronze_movimentorelator, ejud_bronze_tipoprocesso, ejud_bronze_orgao, ejud_bronze_t_processo
import uuid
from utils.facto_auxiliar import FactoAuxiliar 

from dotenv import load_dotenv
PARQUET_HOME = f"/opt/parquet"
load_dotenv(dotenv_path=os.path.join(PARQUET_HOME, "airflow", "dotenv", ".env"),  override=True)
class MetaCalculator:
    """
    Calculator for meta variables (fixed class header to repair indentation issues).
    """

    def __init__(self, spark, logger, config_mgr, config_path=None, meta_number="01", mode="append"):

        self.spark = spark
        self.logger = logger
        self.config_mgr = config_mgr
        self.meta_number = meta_number
        self.mode = mode
        # Only use config JSON for variable lists
        if config_path is None:
            config_path = "./config/variables_config.json"
        with open(config_path, "r", encoding="utf-8") as f:
            config_json = json.load(f)
        self.case1_files = config_json[f"meta_{self.meta_number}"]["variables"]["case1"]
        # Resolve ouro (gold) path for meta_{self.meta_number} using config manager first
        try:
            ejud_path_cfg = self.config_mgr.get_meta_path(f"meta_{self.meta_number}", "ejud_path")
        except Exception:
            ejud_path_cfg = None
        # store resolved ejud path and variable mapping for lazy reading
        self.ejud_path = ejud_path_cfg
        self.variables_config = config_json.get(f"meta_{self.meta_number}", {})
        self.variable_dimension_map = self.variables_config.get("variable_dimension_map", {})
        # __init__ should not perform heavy I/O; reading is done lazily by
        # `read_ouro_parquets()` when the pipeline runs.

    def prepare_distribution_base(self, combined):
        self.logger.info("🏗️  Preparing Shared Distribution Base (Reading movimentorelator)...")
        from pyspark.sql.functions import col, regexp_replace, broadcast

        # 1. Read Raw Paths
        movrelator_path = bronze_movimentorelator()
        orgao_path = ejud_bronze_orgao()
        
        # 2. Read DataFrames
        mov_df = self.spark.read.parquet(movrelator_path) \
            .join(combined.select("id_processo"), col("coddoc") == col("id_processo"), "semi")
        orgao_df = self.spark.read.parquet(orgao_path)
        
        # 3. Join & Select Only What We Need
        joined_df = mov_df.join(
            broadcast(orgao_df.select("codorgjulg")), 
            on="codorgjulg", 
            how="inner"
        ).select(
            col("coddoc").alias("coddoc_movrelator"),
            col("dthrmov").cast("timestamp_ntz"),
            regexp_replace(col("codorgjulg"), r"\\.0$", "").cast("integer").alias("cod_oj"),
            regexp_replace(col("codmagrel"), r"\\.0$", "").cast("integer").alias("cod_magistrado")
        )

        return joined_df

    def read_ouro_parquets(self) -> DataFrame:
        """Read per-variable ouro parquet files and return a single long-format DataFrame.

        The method reads only minimal columns from each variable parquet, maps the
        variable name to `cd_ind` using the variables config, and unions all
        variable frames into a combined long DataFrame.
        """
        from pyspark.sql.functions import col, lit
        from pathlib import Path
        from utils.parquet_utils import get_silver_path

        dfs = {}
        # columns we prefer to read if available
        needed_cols = ["coddoc", "dthrmov", "cod_classe", "nivel_sigilo", "cd_ind", "cod_oj", "sistema", "cod_magistrado", "cod_orgao_julgador"]
        var_map = self.variable_dimension_map or {}

        for fname in self.case1_files:
            prefix = Path(fname).stem
            full_path = str(
                Path('/opt/parquet')
                / 'data'
                / 'prata'
                / 'ejud'
                / 'meta_cnj2026'
                / f"meta_{self.meta_number}"
                / 'current'
                / fname
                / '*.parquet'
            )

            try:
                self.logger.info(f"Reading parquet for '{fname}' from: {full_path}")
                sdf = self.spark.read.parquet(full_path)
            except Exception as e:
                self.logger.warning(f"Failed to read parquet for '{fname}' at {full_path}: {e}")
                continue

            available = sdf.columns
            select_cols = [c for c in needed_cols if c in available]
            if not select_cols:
                self.logger.warning(f"No required columns found in {fname}; skipping")
                continue

            sdf = sdf.select(*[col(c) for c in select_cols])
            sdf = sdf.withColumn("variable_name", lit(prefix))

            mapped = var_map.get(prefix)
            if mapped is not None:
                sdf = sdf.withColumn("cd_ind", lit(int(mapped)).cast('int'))

            dfs[prefix] = sdf

        if not dfs:
            self.logger.error("No parquet variables were loaded from ouro path")
            return None

        union_dfs = []
        for prefix, sdf in dfs.items():
            sel_cols = []
            if 'coddoc' in sdf.columns:
                sel_cols.append(col('coddoc').alias('id_processo'))
            if 'dthrmov' in sdf.columns:
                sel_cols.append(col('dthrmov').alias('dth_evento_indicador'))
            if 'cod_classe' in sdf.columns:
                sel_cols.append(col('cod_classe').alias('cod_classe_cnj'))

            sel_cols.append(col('cd_ind') if 'cd_ind' in sdf.columns else lit(None).cast('int').alias('cod_indicador'))
            sel_cols.append(col('nivel_sigilo') if 'nivel_sigilo' in sdf.columns else lit(None).alias('id_sigilo'))
            sel_cols.append(col('cod_oj').cast('int').alias('cod_oj') if 'cod_oj' in sdf.columns else lit(None).cast('int').alias('cod_oj'))
            sel_cols.append(col('sistema') if 'sistema' in sdf.columns else lit(None).alias('sistema'))
            sel_cols.append(col('cod_magistrado') if 'cod_magistrado' in sdf.columns else lit(None).alias('cod_magistrado'))
            sel_cols.append(col('cod_orgao_julgador') if 'cod_orgao_julgador' in sdf.columns else lit(None).alias('cod_orgao_julgador'))

            union_dfs.append(sdf.select(*sel_cols))

        combined = union_dfs[0]
        for u in union_dfs[1:]:
            combined = combined.unionByName(u, allowMissingColumns=True)

        self.logger.info(f"Read and combined {len(union_dfs)} variable DataFrames")
        
        # Need to filter meta 01 specifics where not can have cod_indicador = 1 if exists distribuicao_df.dthrmov < dthrmov
        # not can have cod_indicador = 2 if exists julgado_df.dthrmov < dthrmov
        julgado_path = f"{get_silver_path('ejud', 'tab_julgamento')}"
        distribuicao_path = f"{get_silver_path('ejud', 'tab_distribuicao')}"
        # Meta 01 specific filtering:
        # - If indicator == 1: drop rows where there exists a distribuicao record
        #   for the same process with dthrmov < dth_evento_indicador
        # - If indicator == 2: drop rows where there exists a julgamento record
        #   for the same process with dthrmov < dth_evento_indicador
        try:
            from pyspark.sql.functions import col

            # The variable column may be named either 'cod_indicador' (expected) or 'cd_ind' (raw).
            ind_col = None
            if "cod_indicador" in combined.columns:
                ind_col = "cod_indicador"
            elif "cd_ind" in combined.columns:
                ind_col = "cd_ind"

            if self.meta_number == "01" and ind_col is not None and "id_processo" in combined.columns and "dth_evento_indicador" in combined.columns:
                # Read distribuicao and julgamento minimal cols
                try:
                    distribuicao_df = self.spark.read.parquet(f"{distribuicao_path}/*.parquet").select(col("coddoc").alias("coddoc_distrib"), col("dthrmov").alias("dthrmov_distrib"))
                except Exception:
                    self.logger.warning(f"Could not read distribuicao parquet at {distribuicao_path}; skipping meta-01 distribuicao filter")
                    distribuicao_df = None

                try:
                    julgado_df = self.spark.read.parquet(f"{julgado_path}/*.parquet").select(col("coddoc").alias("coddoc_julg"), col("dthrmov").alias("dthrmov_julg"))
                except Exception:
                    self.logger.warning(f"Could not read julgamento parquet at {julgado_path}; skipping meta-01 julgamento filter")
                    julgado_df = None

                # Partition combined by indicator value and apply left_anti joins to drop undesired rows
                parts = []

                # Rows with indicator == 1 -> drop if any distribuicao earlier than event
                rows_cod1 = combined.filter(col(ind_col) == 1)
                if distribuicao_df is not None:
                    rows_cod1_clean = rows_cod1.join(distribuicao_df, (rows_cod1.id_processo == distribuicao_df.coddoc_distrib) & (distribuicao_df.dthrmov_distrib < rows_cod1.dth_evento_indicador), how="left_anti")
                else:
                    rows_cod1_clean = rows_cod1
                parts.append(rows_cod1_clean)

                # Rows with indicator == 2 -> drop if any julgamento earlier than event
                rows_cod2 = combined.filter(col(ind_col) == 2)
                if julgado_df is not None:
                    rows_cod2_clean = rows_cod2.join(julgado_df, (rows_cod2.id_processo == julgado_df.coddoc_julg) & (julgado_df.dthrmov_julg < rows_cod2.dth_evento_indicador), how="left_anti")
                else:
                    rows_cod2_clean = rows_cod2
                parts.append(rows_cod2_clean)

                # Other rows (indicator not 1 or 2 or null) keep as-is
                other_rows = combined.filter(~col(ind_col).isin(1, 2) | col(ind_col).isNull())
                parts.append(other_rows)

                # Union back preserving column names
                cleaned = parts[0]
                for p in parts[1:]:
                    cleaned = cleaned.unionByName(p, allowMissingColumns=True)

                combined = cleaned
                self.logger.info("Applied meta-01 distribuicao/julgamento filters")

        except Exception as e:
            self.logger.warning(f"Meta-01 filtering step failed: {e}")
        self.logger.info(f"Read and combined {len(union_dfs)} variable DataFrames")

        return combined

    def _apply_meta08_priority_filtering(self, procass_df: DataFrame) -> DataFrame:
        """
        Apply Meta 08 specific filtering with feminicidio priority over violencia_domestica.
        
        Priority Rules:
        1. If coddoc has assunto in feminicidio codes -> Use feminicidio assunto
        2. If coddoc has assunto in violencia_domestica codes (and not in feminicidio) -> Use violencia_domestica assunto
        3. Deduplicate by coddoc, keeping highest priority assunto
        """
        from pyspark.sql.functions import col, lit, when, min as spark_min
        from pyspark import StorageLevel
        try:
            self.logger.info("📊 Applying Meta 08 priority-based assunto filtering (optimized)...")

            # Get assunto codes from configuration
            feminicidio_codes, violencia_domestica_codes = self._get_meta08_assunto_codes()

            if not feminicidio_codes and not violencia_domestica_codes:
                self.logger.warning("⚠️ No Meta 08 assunto codes found - using default filtering")
                return procass_df.dropDuplicates(["coddoc"])

            self.logger.info(f"🔍 Meta 08 codes - Feminicidio: {len(feminicidio_codes)} codes, Violencia Domestica: {len(violencia_domestica_codes)} codes")
            # Filter to only relevant assunto codes first (dramatically reduces data)
            all_relevant_codes = list(set(feminicidio_codes + violencia_domestica_codes))
            relevant_procass = procass_df.filter(col("codass").isin(all_relevant_codes))

            # Add priority column: feminicidio = 1 (highest), violencia_domestica = 2
            relevant_procass = relevant_procass.withColumn("priority",
                when(col("codass").isin(feminicidio_codes), lit(1))
                .when(col("codass").isin(violencia_domestica_codes), lit(2))
                .otherwise(lit(99))
            )

            try:                
                min_priority_per_coddoc = relevant_procass.groupBy("coddoc").agg(spark_min("priority").alias("min_priority"))

                # Join back to get rows that have the min priority
                joined = relevant_procass.join(min_priority_per_coddoc, relevant_procass.coddoc == min_priority_per_coddoc.coddoc, "inner").select(relevant_procass["*"], min_priority_per_coddoc["min_priority"]) 
                del min_priority_per_coddoc
                # For ties (same priority), pick the smallest numord
                min_numord = joined.filter(col("priority") == col("min_priority")).groupBy("coddoc").agg(spark_min("numord").alias("min_numord"))

                result_df = joined.join(min_numord, (joined.coddoc == min_numord.coddoc) & (joined.numord == min_numord.min_numord), "inner").select(relevant_procass["coddoc"], relevant_procass["codass"], relevant_procass["numord"]).distinct()
                del min_numord
                del joined
                
                return result_df

            except Exception as agg_err:
                self.logger.warning(f"⚠️ Optimized priority aggregation failed ({agg_err}), falling back to window method")
                
        except Exception as e:
            self.logger.error(f"❌ Error in Meta 08 priority filtering: {e}")
            self.logger.warning("⚠️ Falling back to standard deduplication")
            return procass_df.dropDuplicates(["coddoc"])

    def _get_meta08_assunto_codes(self) -> tuple:
        """Get Meta 08 feminicidio and violencia_domestica assunto codes from configuration."""
        try:
            config_dir = Path(__file__).parent.parent / "meta_cnj2025/config"
            
            with open(config_dir / "assunto_groups.json", 'r', encoding='utf-8') as f:
                assunto_config = json.load(f)
            
            feminicidio_codes = assunto_config.get("assuntos_grupos", {}).get("feminicidio", [])
            violencia_domestica_codes = assunto_config.get("assuntos_grupos", {}).get("violencia_domestica", [])
            
            return feminicidio_codes, violencia_domestica_codes
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load Meta 08 assunto codes: {e}")
            return [], []

    def _get_meta_relevant_codass(self, meta_num: str) -> List[int]:
        """Get relevant codass values for meta with proper error handling."""
        try:
            config_dir = Path(__file__).parent.parent / "meta_cnj2025/config"
            
            # Load configurations
            with open(config_dir / "assunto_groups.json", 'r', encoding='utf-8') as f:
                assunto_config = json.load(f)
            
            with open(config_dir / "meta_config.json", 'r', encoding='utf-8') as f:
                meta_config = json.load(f)
            
            # Get filter paths for this meta
            meta_key = f"meta_{meta_num}"
            if meta_key not in meta_config:
                return []
            
            meta_section = meta_config[meta_key]
            filter_paths_key = f"meta{meta_num}_filter_paths"
            
            filter_paths = (meta_section.get("paths", {}).get(filter_paths_key) or 
                          meta_section.get(filter_paths_key, []))
            
            if not filter_paths:
                return []
            
            # Extract codass values efficiently
            all_codass = set()
            
            for path in filter_paths:
                filename = Path(path).stem
                if filename.startswith("filtered_coddoc_"):
                    subject_name = filename[len("filtered_coddoc_"):]
                    
                    # Check both assuntos_grupos and classes
                    for config_key in ["assuntos_grupos", "classes"]:
                        if config_key in assunto_config and subject_name in assunto_config[config_key]:
                            codass_list = assunto_config[config_key][subject_name]
                            all_codass.update(codass_list)
            
            result = sorted(list(all_codass))
            self.logger.info(f"Meta {meta_num}: Found {len(result)} relevant codass values")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to get relevant codass for meta {meta_num}: {e}")
            return []
        
    def enrich_with_processoassunto(self, combined_df):
        from pyspark.sql.functions import col, lit, row_number, broadcast, max
        try:
            # Read processoassunto table
            processoassunto_path = get_bronze_path("ejud", "processoassunto")
            self.logger.info(f"Reading processoassunto from: {processoassunto_path}")
            
            processoassunto_df = self.spark.read.parquet(f"{processoassunto_path}/*.parquet") \
                .join(combined_df.select("id_processo"), col("id_processo") == col("coddoc"), "semi")
            
            
            if self.meta_number in ["01", "02"]:
                # Filter for principal assunto (numord = 1) and select needed columns
                processoassunto_df = processoassunto_df.filter(
                    processoassunto_df.numord == 1
                ).withColumnRenamed("codass", "id_assunto_principal").drop("numord")
                self.logger.info(f"Found principal assuntos (numord=1)")
                return processoassunto_df
            
            # Meta 08: prefer the same relevant-codass filtering used by other metas when available,
            # otherwise fall back to the priority-based logic (feminicidio > violencia_domestica).
            elif self.meta_number == "08":
                try:
                    relevant_codass = self._get_meta_relevant_codass(self.meta_number)
                    if relevant_codass:
                        self.logger.info(f"Meta 08: Filtering processoassunto by {len(relevant_codass)} relevant codass (meta config)")
                        processoassunto_df = processoassunto_df.filter(col("codass").isin(relevant_codass)).drop("numord")
                        processoassunto_df = processoassunto_df.dropDuplicates(["coddoc"]).withColumnRenamed("codass", "id_assunto_principal")
                        return processoassunto_df
                    else:
                        # no relevant codass configured for meta 08; fallback to the priority rules
                        self.logger.info("Meta 08: No relevant codass configured; falling back to priority-based assunto selection")
                        processoassunto_df = self._apply_meta08_priority_filtering(processoassunto_df)
                        processoassunto_df = processoassunto_df.withColumnRenamed("codass", "id_assunto_principal").drop("numord")
                        return processoassunto_df
                except Exception as e:
                    self.logger.warning(f"Meta 08: erro ao processar processoassunto; falling back to simple dedupe: {e}")
                    processoassunto_df = processoassunto_df.dropDuplicates(["coddoc"]).withColumnRenamed("codass", "id_assunto_principal")
                    return processoassunto_df
            
            # For other metas: filter by relevant codass first, then deduplicate
            elif self.meta_number in ["04", "06", "07", "10", "11"]:
                relevant_codass = self._get_meta_relevant_codass(self.meta_number)
                if relevant_codass:
                    self.logger.info(f"Meta {self.meta_number}: Filtering by {len(relevant_codass)} relevant codass")
                    processoassunto_df = processoassunto_df.filter(col("codass").isin(relevant_codass)).drop("numord")
                else:
                    self.logger.info(f"Meta {self.meta_number}: No relevant codass configured, using all assuntos with numord=1")
                    processoassunto_df = processoassunto_df.filter(processoassunto_df.numord == 1).drop("numord")
                processoassunto_df = processoassunto_df.dropDuplicates(["coddoc"])
                processoassunto_df = processoassunto_df.withColumnRenamed("codass", "id_assunto_principal")
                self.logger.info("✅ Successfully enriched records with id_assunto_principal (where available)")
                return processoassunto_df
            
        except Exception as e:
            self.logger.error(f"Failed to enrich with processoassunto: {e}")
            return combined_df
    
    def join_with_processes(self, df_filter) -> DataFrame:
        from pathlib import Path
        from pyspark.sql.functions import coalesce, lit, col, lpad, concat, first, when, row_number
        from utils.parquet_utils import ejud_bronze_competencia
        
        t_proc_path = ejud_bronze_t_processo()
        
        t_proc_df = self.spark.read.parquet(t_proc_path) \
            .join(df_filter, col("coddoc") == col("id_processo"), "semi") \
            .select("coddoc", "numsequencial", "dv", "numano", "codjustica", "codregi", "codlocregi", "codcompt")
        
        t_proc_df = self.mount_num_processo(t_proc_df)
        t_proc_df = t_proc_df.withColumnRenamed("coddoc", "p_coddoc")
        
        return t_proc_df

    def join_with_movrelator(self, distrib_df, cutoff_date=None) -> DataFrame:
        from pyspark.sql.functions import col, row_number, regexp_replace, coalesce, when, max, broadcast, lit
        from pyspark.sql.window import Window

        self.logger.info("Preparing movrelator enrichment data...")

        if cutoff_date is not None:
            distrib_df = distrib_df.filter(col("dthrmov") <= lit(cutoff_date))

        # Create window specifications for ascending and descending order
        w_desc = Window.partitionBy("coddoc_movrelator").orderBy(col("dthrmov").desc())
        w_asc = Window.partitionBy("coddoc_movrelator").orderBy(col("dthrmov").asc())

        # Calculate ranks for first and last events
        distrib_ranked = distrib_df.withColumn("rn_desc", row_number().over(w_desc)) \
                                   .withColumn("rn_asc", row_number().over(w_asc))
        
        # Filter to keep only the first and last rows for each process, optimizing the aggregation
        distrib_filtered = distrib_ranked.filter((col("rn_desc") == 1) | (col("rn_asc") == 1))

        # Perform conditional aggregation to pivot first and last event data into columns
        movrel_agg = distrib_filtered.groupBy("coddoc_movrelator").agg(
            max(when(col("rn_desc") == 1, col("dthrmov"))).alias("dth_ult_distribuicao"),
            max(when(col("rn_desc") == 1, col("cod_oj"))).alias("cod_ult_oj"),
            max(when(col("rn_desc") == 1, col("cod_magistrado"))).alias("cod_ult_magistrado"),
            max(when(col("rn_asc") == 1, col("dthrmov"))).alias("dth_prim_distribuicao"),
            max(when(col("rn_asc") == 1, col("cod_oj"))).alias("cod_prim_oj"),
            max(when(col("rn_asc") == 1, col("cod_magistrado"))).alias("cod_prim_magistrado")
        )

        # Coalesce values to ensure primary fields are populated from ultimate fields if they are null
        movrel_agg = movrel_agg.withColumn(
            "cod_prim_oj",
            coalesce(movrel_agg["cod_prim_oj"], movrel_agg["cod_ult_oj"])
        ).withColumn(
            "cod_prim_magistrado",
            coalesce(movrel_agg["cod_prim_magistrado"], movrel_agg["cod_ult_magistrado"])
        ).withColumn(
            "dth_prim_distribuicao",
            coalesce(movrel_agg["dth_prim_distribuicao"], movrel_agg["dth_ult_distribuicao"])
        )
        
        self.logger.info(f'join_with_movrelator returning columns: {movrel_agg.columns}')

        return movrel_agg

    def mount_num_processo(self, processo_df) -> DataFrame:
        from pyspark.sql.functions import col, lpad, concat
        self.logger.info('montando num_processo')
        t_proc_df = processo_df
        t_proc_df = t_proc_df.withColumn("numsequencial", col("numsequencial").cast("string"))
        t_proc_df = t_proc_df.withColumn("dv", col("dv").cast("string"))
        t_proc_df = t_proc_df.withColumn("codlocregi", col("codlocregi").cast("string"))
        t_proc_df = t_proc_df.withColumn("numsequencial_padded", lpad(col("numsequencial"), 7, '0'))
        t_proc_df = t_proc_df.withColumn("dv_padded", lpad(col("dv"), 2, '0'))
        t_proc_df = t_proc_df.withColumn("codlocregi_padded", lpad(col("codlocregi"), 4, '0'))
        t_proc_df = t_proc_df.withColumn(
            "num_processo",
            concat(
                col("numsequencial_padded"),
                col("dv_padded"),
                col("numano").cast("string"),
                col("codjustica").cast("string"),
                col("codregi").cast("string"),
                col("codlocregi_padded")
            )
        ).drop("numsequencial_padded", "dv_padded", "numano", "codjustica", "codregi", "codlocregi_padded", "dv", "numsequencial", "codlocregi")

        return t_proc_df

    def rename_and_ordercolumns(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col, when, lower, lit, current_timestamp, regexp_replace
        import time
        # Defensive: ensure we were passed a DataFrame-like object
        self.logger.info(f"Renaming and ordering columns...{df.columns}")

        if df is None:
            self.logger.error("rename_and_ordercolumns called with df=None")
            return None
        try:
            if "id_classe_judicial" in df.columns and "cod_classe_cnj" not in df.columns:
                df = df.withColumnRenamed("id_classe_judicial", "cod_classe_cnj")
            
            # cod_indicador
            if "cd_ind" in df.columns and "cod_indicador" not in df.columns:
                df = df.withColumnRenamed("cd_ind", "cod_indicador")

            # id_sigilo
            if "nivel_sigilo" in df.columns and "id_sigilo" not in df.columns:
                df = df.withColumnRenamed("nivel_sigilo", "id_sigilo")

            # cod_sistema map textual 'sistema' -> 1/2
            if "sistema" in df.columns and "cod_sistema" not in df.columns:
                df = df.withColumn(
                    "cod_sistema",
                    when(lower(col("sistema")).rlike("^ejud"), lit(1))
                    .when(lower(col("sistema")).rlike("^eproc"), lit(2))
                    .otherwise(lit(None))
                )
            
            # Ensure dth_atualizacao exists: use existing, else current_timestamp()
            if "dth_atualizacao" not in df.columns:
                df = df.withColumn("dth_atualizacao", current_timestamp())

            

            return df
        
        except Exception as e:
            import traceback
            self.logger.error(f"Not able to rename and order dataframe columns: {e}")
            return None
    
    def join_with_ultimadistribuicaoriger(self, combined_df, cutoff_date=None) -> DataFrame:
        from utils.parquet_utils import get_ultimadistribuicaoriger_path, ejud_bronze_orgao
        from pyspark.sql.functions import col, broadcast, struct, min as spark_min, max as spark_max, lit, current_date, date_sub, to_date

        ultimadistribuicaoriger_path = get_ultimadistribuicaoriger_path("ejud")
        orgao_path = ejud_bronze_orgao()

        self.logger.info("Reading and pre-filtering situacao table")
        try:
            orgao_df = self.spark.read.parquet(orgao_path)
            self.logger.info(f"Reading ejud_situacao parquet")
            # Keep the original codorgjulg as the canonical `cod_oj` (do not remap to codorgjulgcnj)
            situacao_df = self.spark.read.parquet(ultimadistribuicaoriger_path) \
                .select(
                    col("coddoc"),
                    col("dthrmov").cast("timestamp_ntz"),
                    col("codmagrel").cast("integer"),
                    col("codorgjulg").alias("cod_oj")
                ) \
                .join(
                    broadcast(combined_df.select("id_processo")),
                    col("coddoc") == col("id_processo"),
                    "semi"
                )

            if cutoff_date is not None:
                situacao_df = situacao_df.filter(col("dthrmov") <= lit(cutoff_date))

            self.logger.info(f"Using original codorgjulg as cod_oj in situacao_df")
            # Create a struct to keep related columns together for min/max aggregation
            event_struct = struct(col("dthrmov"), col("cod_oj"), col("codmagrel"))

            # Group by coddoc and get the min and max event struct. This is much faster than two window functions.
            agg_df = situacao_df.groupBy("coddoc").agg(
                spark_min(event_struct).alias("first_event"),
                spark_max(event_struct).alias("last_event")
            )

            # Expand the structs back into columns
            result_df = agg_df.select(
                col("coddoc"),
                col("first_event.dthrmov").alias("dth_prim_distribuicao"),
                col("first_event.cod_oj").alias("cod_prim_oj"),
                col("first_event.codmagrel").alias("cod_prim_magistrado"),
                col("last_event.dthrmov").alias("dth_ult_distribuicao"),
                col("last_event.cod_oj").alias("cod_ult_oj"),
                col("last_event.codmagrel").alias("cod_ult_magistrado")
            )

            result_df = result_df.withColumnRenamed("coddoc", "coddoc_ultimadistribuicaoriger") 

            # Select and alias the final columns to be returned
            result_df = result_df.select(
                col("coddoc_ultimadistribuicaoriger"),
                col("dth_prim_distribuicao").alias("ejud_dth_prim"),
                col("dth_ult_distribuicao").alias("ejud_dth_ult"),
                col("cod_prim_oj").alias("ejud_oj_prim"),
                col("cod_ult_oj").alias("ejud_oj_ult"),
                col("cod_prim_magistrado").alias("ejud_mag_prim"),
                col("cod_ult_magistrado").alias("ejud_mag_ult")
            )
            
            self.logger.info(f"✅ Prepared ejud_situacao data successfully using optimized method.")
            return result_df
        
        except Exception as e:
            self.logger.error(f"❌ Failed to prepare ejud_situacao data: {e}")
            return combined_df
        
    
    def normalize_and_save(self) -> DataFrame:
        import shutil
        import gc
        from pathlib import Path
        from pyspark.sql.functions import coalesce, lit, col, lpad, concat, first, when, current_timestamp, broadcast, row_number, monotonically_increasing_id, regexp_replace, trim
        from pyspark.storagelevel import StorageLevel

        # CRITICAL: Clear Spark cache at the very beginning to ensure fresh start
        self.logger.info(f"🧹 Meta {self.meta_number}: Starting with comprehensive cache clear...")
        
        # Step 1: Read and aggregate parquet variables
        self.logger.info("Start read variables")
        combined = self.read_ouro_parquets()
        if combined is None:
            self.logger.error("No data returned from read_ouro_parquets; aborting")
            return False
        # Enrich combined with tipoprocesso (cod_natureza) and orgao (cod_competencia)
        from pyspark.sql.functions import broadcast, col, lit
        try:
            tipoprocesso_path = ejud_bronze_tipoprocesso()
            tipoprocesso_df = self.spark.read.parquet(tipoprocesso_path).select("codextcnj", "codnat")
            # Normalize cod_classe values to match tipoprocesso.codextcnj (strip .0 suffix and cast to string)
            from pyspark.sql.functions import regexp_replace
            if 'cod_classe_cnj' in combined.columns:
                combined = combined.withColumn("_cod_classe_norm", regexp_replace(col("cod_classe_cnj"), r"\.0$", ""))
                # ensure both sides comparable as string
                tipoprocesso_df = tipoprocesso_df.withColumn("_codextcnj_norm", regexp_replace(col("codextcnj"), r"\.0$", ""))
                combined = combined.join(
                    broadcast(tipoprocesso_df.select("_codextcnj_norm", "codnat")),
                    combined._cod_classe_norm == tipoprocesso_df._codextcnj_norm,
                    how="left"
                ).drop("_codextcnj_norm").withColumnRenamed("codnat", "cod_natureza").drop("_cod_classe_norm")
            else:
                self.logger.info("Skipping tipoprocesso enrichment: 'cod_classe_cnj' not present in combined DataFrame")
        except Exception as e:
            self.logger.warning(f"Failed to enrich with tipoprocesso: {e}")

        try:
            orgao_path = ejud_bronze_orgao()
            # Read orgao using original codorgjulg (we will match directly with cod_oj)
            orgao_df = self.spark.read.parquet(orgao_path).select("codcompt", "codorgjulg")
            # Normalize cod_oj and codorgjulg to comparable form (strip .0 and cast to integer where possible)
            from pyspark.sql.functions import regexp_replace
            if 'cod_oj' in combined.columns:
                combined = combined.withColumn("_cod_oj_norm", regexp_replace(col("cod_oj"), r"\.0$", ""))
                orgao_df = orgao_df.withColumn("_codorg_norm", regexp_replace(col("codorgjulg"), r"\.0$", ""))
                combined = combined.join(
                    broadcast(orgao_df.select("_codorg_norm", "codcompt")),
                    combined._cod_oj_norm == orgao_df._codorg_norm,
                    how="left"
                ).drop("_codorg_norm").withColumnRenamed("codcompt", "cod_competencia").drop("_cod_oj_norm")
            else:
                self.logger.info("Skipping orgao enrichment: 'cod_oj' not present in combined DataFrame")
        except Exception as e:
            self.logger.warning(f"Failed to enrich with orgao: {e}")

        # Ensure a default cod_sistema when not provided
        if 'cod_sistema' not in combined.columns:
            combined = combined.withColumn('cod_sistema', lit(1))

        combined = combined.localCheckpoint()
        self.logger.info(f"Completed reading variables: {combined.count():,} records with columns {combined.columns}")
        
        # Step 2: Enrich with processoassunto
        self.logger.info("Enriching combined DataFrame with processoassunto...")
        procass_df = self.enrich_with_processoassunto(combined)
        self.logger.info("Checkpointing 'procass_df' DataFrame after enriching with processoassunto.")        
        procass_df = procass_df.localCheckpoint()
        self.logger.info("Starting distribution data enrichment to get columns from movimentorelator and if the cod_prim_magistrado is null so we use ejud_situacao")
        distrib_base_df = self.prepare_distribution_base(combined)
        
        oj_mag_mapping = distrib_base_df.select(
            col("cod_oj").alias("codorgjulg"),
            col("cod_magistrado").alias("codmagrel")
        )
        # Cutoff: movements beyond the target year must not influence cod_ult_magistrado / cod_ult_oj.
        from datetime import date as _date
        _target_year = 2026
        _mov_cutoff = _date(_target_year, 12, 31)

        # Get data from movrelator and ejud_situacao sources independently
        self.logger.info("Preparing movrelator and ejud_situacao data...")
        relator_df = self.join_with_movrelator(distrib_base_df, cutoff_date=_mov_cutoff)
        
        relator_df = relator_df.localCheckpoint()
        self.logger.info(f'movrelator_df columns: {relator_df.columns}')
        
        # Join the prepared dataframes to the main dataframe `df`
        self.logger.info("Joining processoassunto, movrelator and ejud_situacao data to main DataFrame...")
        df = combined.join(relator_df, combined["id_processo"] == relator_df["coddoc_movrelator"], how="left").drop("coddoc_movrelator")
        del relator_df
        gc.collect()
        df = df.localCheckpoint()

        # Join processoassunto enrichment (id_assunto_principal) so FactoAuxiliar can use it
        try:
            if procass_df is not None:
                if 'coddoc' in procass_df.columns:
                    # Defensive join: cast both sides to string and trim to avoid type/format mismatches
                    from pyspark.sql.functions import trim
                    procass_small = procass_df.select(procass_df.coddoc.cast('string').alias('proc_coddoc'), 'id_assunto_principal').dropDuplicates(['proc_coddoc'])
                    df = df.join(procass_small, df['id_processo'].cast('string') == procass_small['proc_coddoc'], how='left').drop('proc_coddoc')
                    try:
                        matched = df.filter(col('id_assunto_principal').isNotNull()).count()
                        self.logger.info(f"After join: {matched:,} rows have id_assunto_principal populated")
                    except Exception:
                        self.logger.debug("Skipping matched-count after join (expensive)")
                else:
                    # fallback: try renaming if different column name
                    self.logger.warning("procass_df missing 'coddoc' column; skipping join of processoassunto")
        except Exception as e:
            self.logger.warning(f"Failed to join processoassunto to main df: {e}")

        situacao_df = self.join_with_ultimadistribuicaoriger(combined, cutoff_date=_mov_cutoff)
        del combined
        gc.collect()

        self.logger.info(f'situacao_df columns: {situacao_df.columns}')
        
        df = df.join(situacao_df, df["id_processo"] == situacao_df["coddoc_ultimadistribuicaoriger"], how="left").drop("coddoc_ultimadistribuicaoriger")
        # Now, perform the coalesce logic
        
        del situacao_df
        gc.collect()
        self.logger.info("Coalescing distribution data from movrelator and ejud_situacao sources...")
        df = df.withColumn("dth_prim_distribuicao", coalesce(col("dth_prim_distribuicao"), col("ejud_dth_prim"))) \
               .withColumn("dth_ult_distribuicao",  coalesce(col("dth_ult_distribuicao"),  col("ejud_dth_ult"))) \
               .withColumn("cod_prim_oj",           coalesce(col("cod_prim_oj"),           col("ejud_oj_prim"))) \
               .withColumn("cod_ult_oj",            coalesce(col("cod_ult_oj"),            col("ejud_oj_ult"))) \
               .withColumn("cod_prim_magistrado",   coalesce(col("cod_prim_magistrado"),   col("ejud_mag_prim"))) \
               .withColumn("cod_ult_magistrado",    coalesce(col("cod_ult_magistrado"),    col("ejud_mag_ult")))

        # Clean up helper columns from situacao_df join
        df = df.drop(
            "ejud_dth_prim", "ejud_dth_ult", "ejud_oj_prim",
            "ejud_oj_ult", "ejud_mag_prim", "ejud_mag_ult"
        )
        
        # Strategic checkpoint after the first set of heavy joins
        self.logger.info("Checkpointing DataFrame after movrelator and ejud_situacao joins.")
        string_cols = [c for c, t in df.dtypes if "string" in t.lower()]
        if string_cols:
            self.logger.info(f"Trimming {len(string_cols)} string columns to reduce unexpected large values")
            for c in string_cols:
                df = df.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))
        df = df.localCheckpoint()
        # Normalize column names to expected canonical names (e.g., id_sigilo, cod_indicador)
        try:
            df = self.rename_and_ordercolumns(df)
        except Exception as e:
            self.logger.warning(f"rename_and_ordercolumns failed: {e}")
        
        # Step 4: Final processing and splitting the pipeline for two outputs
        try:
            # cast integer-like cols to Integer where appropriate (clean .0 suffix first)
            for intcol in ["cod_magistrado", "cod_orgao_julgador", "cod_classe_cnj", "id_assunto_principal", "cod_competencia", "cod_indicador", "id_sigilo", "cod_sistema", "cod_natureza", "cod_ult_oj", "cod_ult_magistrado"]:
                if intcol in df.columns:
                    # Clean .0 suffix then cast to Integer to avoid PostgreSQL conversion errors
                    df = df.withColumn(intcol, regexp_replace(col(intcol), r"\.0$", "").cast("Integer"))
            self.logger.info(f'Before to rename columns: {df.columns}')


            aux_calc = FactoAuxiliar(df=df, spark=self.spark, logger=self.logger, meta_number=self.meta_number, mode=self.mode)
            success_aux = aux_calc.build_and_save()

            if not success_aux:
                self.logger.error("Facto Auxiliar failed, aborting pipeline.")
                return False
            
            self.logger.info("Facto Auxiliar saved successfully.")

            # Now, create the final DataFrame for meta_analitico, which DOES need 'processo' info
            self.logger.info("Performing final join with t_processo for meta_analitico output.")
            processo_df = self.join_with_processes(df.select("id_processo"))
            final_df = df.join(processo_df, df["id_processo"] == processo_df["p_coddoc"], how="inner").drop("p_coddoc")

            # Coalesce cod_competencia from processo if missing
            final_df = final_df.withColumn("cod_competencia", coalesce(final_df["cod_competencia"], final_df["codcompt"])).drop("codcompt")
            # Step 5: Final Save to Postgres (meta_analitico)
            self.logger.info("Step 5: Final Formatting and Save to meta_analitico...")
            final_df = final_df.withColumn("dth_atualizacao", current_timestamp())
            final_df = final_df.withColumn("id_processo", final_df["id_processo"].cast("string"))
            
            target_year = 2026
            final_df = final_df.withColumn(
                "meta_ano",
                lit(target_year)
            )
            
            ordered = ["id_processo", "num_processo", "cod_sistema", "cod_classe_cnj",
                    "id_assunto_principal", "cod_competencia", "dth_evento_indicador",
                    "cod_indicador", "id_sigilo", "cod_natureza", "dth_atualizacao",
                    "cod_ult_oj", "cod_ult_magistrado", "meta_ano"]
            
            # Ensure all required columns are present before selecting
            for col_name in ordered:
                if col_name not in final_df.columns:
                    self.logger.warning(f"Column '{col_name}' not found in final DataFrame for meta_analitico. It will be added as null.")
                    final_df = final_df.withColumn(col_name, lit(None))
            
            final_df = final_df.select(*ordered)

            stringcols = [c for c, t in final_df.dtypes if "string" in t.lower()]
            if stringcols:
                self.logger.info(f"Trimming {len(stringcols)} string columns before final save.")
                for c in stringcols:
                    final_df = final_df.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))

            # Quick check for conflicting cod_natureza values per (id_processo, dth_evento_indicador, cod_indicador)
            try:
                from pyspark.sql.functions import countDistinct
                conflict_groups = final_df.groupBy("id_processo", "dth_evento_indicador", "cod_indicador").agg(countDistinct("cod_natureza").alias("n_distinct_cod_natureza")).filter(col("n_distinct_cod_natureza") > 1)
                conflict_count = conflict_groups.count()
                if conflict_count > 0:
                    self.logger.warning(f"Found {conflict_count:,} groups with conflicting cod_natureza values. Applying deterministic deduplication (prefer non-null cod_natureza).")
            except Exception:
                self.logger.debug("cod_natureza conflict check failed; continuing to deterministic dedupe")

            # Deterministic deduplication: keep one row per (id_processo, dth_evento_indicador, cod_indicador)
            try:
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number, when

                self.logger.info("Applying deterministic deduplication by id_processo, dth_evento_indicador, cod_indicador (prefer cível cod_natureza=1)")
                # Prefer rows where cod_natureza == 1 (cível), then other non-null cod_natureza, then null.
                dedupe_window = Window.partitionBy("id_processo", "dth_evento_indicador", "cod_indicador").orderBy(
                    when(col("cod_natureza") == 1, 0)
                        .when(col("cod_natureza").isNotNull(), 1)
                        .otherwise(2),
                    # next prefer rows that have ultimate oj (more complete distribution info)
                    when(col("cod_ult_oj").isNotNull(), 0).otherwise(1),
                    # finally prefer most recent update
                    col("dth_atualizacao").desc()
                )

                final_df = final_df.withColumn("_rn", row_number().over(dedupe_window)).filter(col("_rn") == 1).drop("_rn")
                
            except Exception as dedupe_err:
                self.logger.warning(f"Deterministic deduplication failed: {dedupe_err}; proceeding without it")

            if "cod_indicador" in df.columns:
                # For the same dataframe just will exists 1 or more cod_indicador (integer)
                cod_indicador = final_df.select("cod_indicador").distinct().rdd.flatMap(lambda x: x).collect()
                cod_sistema_list = final_df.select("cod_sistema").distinct().rdd.flatMap(lambda x: x).collect()
                delete_cond = f"meta_ano = {target_year} and cod_sistema in ({', '.join(map(str, cod_sistema_list))}) and cod_indicador in ({', '.join(map(str, cod_indicador))})"
                self.logger.info(f"Will delete existing rows for meta_ano={target_year} before inserting (meta {self.meta_number})")
            else:
                delete_cond = None
                self.logger.info(f"Meta {self.meta_number}: appending without deleting prior rows (meta_ano fixed to {target_year})")

            is_saved = save_df_to_postgres_no_jar(final_df, schema="dw2inst", table_name="meta_analitico", mode=self.mode, delete_condition=delete_cond)
        
            if is_saved:
                return True
            
            else:
                self.logger.error("Failed to save final DataFrame to meta_analitico.")
                return False
        except Exception as e:
            import traceback
            self.logger.error(f"Failed pipeline in final processing stage: {e}")
            self.logger.debug(traceback.format_exc())
            return False