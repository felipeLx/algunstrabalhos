from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from utils.parquet_utils import meta_current
import json
from utils.debug_utils import is_debug, debug_log_count
        
class EprocMetaCalculator:
    def __init__(self, spark, logger, config_mgr, config_path=None, meta_number="01_eproc", mode="append"):
        

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
        self.case1_files = config_json["meta_01"]["variables"]["case1"]
        
        self.eproc_path = meta_current("01","eproc2g")

        # Initialize FactoAuxiliar
        self.logger.info(f"Loaded variables config from: {config_path}")
        self.logger.info(f"Resolved EPROC path: {self.eproc_path}")
        self.logger.info(f"case1 parquet files: {self.case1_files}")
        # Load assuntos_groups.json (optional) to get classification lists such as 'conhecimento'
        try:
            assuntos_path = "./config/assuntos_groups.json"
            with open(assuntos_path, "r", encoding="utf-8") as af:
                assuntos_json = json.load(af)
            # Build a list/set of "conhecimento" class codes if present
            conhecimento_codes = []
            if "classes" in assuntos_json and "conhecimento" in assuntos_json["classes"]:
                conhecimento_codes = assuntos_json["classes"]["conhecimento"]
            # Ensure we have a Python list of ints (or strings) usable in Spark .isin()
            self.assuntos_conhecimento_list = [int(x) for x in conhecimento_codes]
            self.logger.info(f"Loaded {len(self.assuntos_conhecimento_list)} conhecimento class codes from {assuntos_path}")
        except Exception:
            self.assuntos_conhecimento_list = []
            self.logger.warning("Could not load config/assuntos_groups.json - proceeding without conhecimento filtering")

    def read_prata_parquets(self) -> DataFrame:
        import os
        from pathlib import Path
        from utils.parquet_utils import parquet_path
        from pyspark.sql.functions import col, lit

        dfs = {}
        for fname in self.case1_files:
            def safe_join(base, name):
                if not base:
                    return parquet_path(name)
                if isinstance(base, str) and base.startswith("s3a://"):
                    return base.rstrip("/") + "/" + name.lstrip("/")
                if isinstance(base, str) and os.path.isabs(base):
                    return os.path.normpath(os.path.join(base, name))
                return parquet_path(os.path.join(base, name))

            full_path = safe_join(self.eproc_path, fname)
            self.logger.info(f"Reading prata parquet: {full_path}")
            try:
                # Read with recursive lookup for directories
                df = self.spark.read.option("recursiveFileLookup", "true").parquet(full_path)
                self.logger.info(f"Columns in {fname}: {df.columns}")
                
                # CRITICAL: Deduplication - Remove duplicate id_processo within each file
                df_deduped = df.dropDuplicates(["id_processo"])
                
                # Add source indicator to track which variable file this came from
                df = df.withColumn("source_variable", lit(Path(fname).stem))
                
                # CRITICAL: Add cod_indicador BEFORE union (when source_variable is clean)
                eproc_var_map = {
                    'cnm01': 1,
                    'julgadom01': 2, 
                    'susm01': 3,
                    'desm01': 4,
                    'PendJulg01TG': 5
                }
                
                # Map source_variable to cod_indicador immediately
                source_var = Path(fname).stem
                if source_var in eproc_var_map:
                    cod_indicador_value = eproc_var_map[source_var]
                    df = df.withColumn("cod_indicador", lit(cod_indicador_value))
                    self.logger.info(f"✅ Added cod_indicador={cod_indicador_value} for {source_var}")
                else:
                    df = df.withColumn("cod_indicador", lit(None).cast("int"))
                    self.logger.warning(f"⚠️ No cod_indicador mapping for {source_var}")
                
                
                prefix = Path(fname).stem
                dfs[prefix] = df
            except Exception as e:
                self.logger.warning(f"Failed to read {full_path}: {e}")
                
        #Create PendJulg01TG variable as set difference between cnm01 and julgadom01 ---
        if f"cnm01" in dfs and "julgadom01" in dfs:
            cnm_df = dfs[f"cnm01"]
            julgadom_df = dfs[f"julgadom01"]
            pendjulg_df = cnm_df.join(julgadom_df.select("id_processo"), on="id_processo", how="left_anti")
            pendjulg_df = pendjulg_df.withColumn(f"PendJulg01TG_present", lit(1))
            
            # CRITICAL: Add cod_indicador for calculated PendJulg01TG variable
            pendjulg_df = pendjulg_df.withColumn("cod_indicador", lit(5))  # PendJulg01TG = 5
            pendjulg_df = pendjulg_df.withColumn("source_variable", lit("PendJulg01TG"))
            self.logger.info("✅ Added cod_indicador=5 for calculated PendJulg01TG variable")
            
            renamed_cols = []
            for c in pendjulg_df.columns:
                if c == "id_processo":
                    renamed_cols.append("id_processo")
                elif c == f"PendJulg01TG_present":
                    renamed_cols.append(f"PendJulg01TG_present")
                elif c in ["cod_indicador", "source_variable"]:
                    # Keep these columns as-is for the calculated variable
                    renamed_cols.append(c)
                else:
                    new_name = f"PendJulg01TG_{c}" if not c.endswith("_present") else c
                    pendjulg_df = pendjulg_df.withColumnRenamed(c, new_name)
                    renamed_cols.append(new_name)
            select_cols = ["id_processo"] + [c for c in renamed_cols if c != "id_processo"]
            dfs[f"PendJulg01TG"] = pendjulg_df.select(*select_cols)
            self.logger.info("Created PendJulg01TG variable with cod_indicador=5 (row count suppressed unless debug)")
            
        return dfs
   
    def normalize_and_save(self) -> bool:
        """
        Main ETL pipeline for eproc2g system - builds both meta_analitico and fato_auxiliar
        """
        from pathlib import Path
        from pyspark.sql.functions import coalesce, lit, col, current_timestamp
        from utils.save_to_postgres import save_parquet_to_postgres, save_df_to_postgres_no_jar

        import time
        import traceback

        try:
            self.logger.info("Starting eproc2g ETL pipeline...")
            
            # Step 1: Read eproc2g variable data (already aggregated in eproc2g format)
            dfs = self.read_prata_parquets()
            
            # Step 2: Build main DataFrame with eproc2g data structure
            main_df = None
            all_columns = set()
            
            # First pass: collect all unique columns from all DataFrames
            for prefix, df in dfs.items():
                all_columns.update(df.columns)
                self.logger.info(f"DataFrame {prefix} has columns: {df.columns}")
                    
            # Add cod_indicador to all_columns set to ensure it gets included
            all_columns.add("cod_indicador")
            
            # Second pass: union all DataFrames with standardized columns
            for prefix, df in dfs.items():
                # Add missing columns as null, but preserve cod_indicador if it exists
                for col_name in all_columns:
                    if col_name not in df.columns:
                        if col_name == "cod_indicador":
                            # cod_indicador should already be added, but add as null if missing
                            df = df.withColumn(col_name, lit(None).cast("int"))
                        else:
                            df = df.withColumn(col_name, lit(None))
                
                # Ensure consistent column order
                df = df.select(*sorted(all_columns))
                
                if main_df is None:
                    main_df = df
                else:
                    main_df = main_df.union(df)
            
            if main_df is None:
                self.logger.error("Could not build main DataFrame from eproc2g data")
                return False
            
            # Step 3: Enrich with missing columns (cod_competencia, cod_natureza, cod_sistema, id_sigilo, cod_prim_magistrado, cod_prim_oj, cod_ult_magistrado, cod_ult_oj)
            main_df = self.enrich_with_competencia_and_natureza(main_df)
            # First get the assunto_judicial mapping for id_assunto_principal
            main_df = self.enrich_with_assunto_judicial(main_df)
            
            # Step 4: Build meta_analitico DataFrame
            meta_analitico_df = self.build_meta_analitico(main_df)
            
            # Step 5: Build fato_auxiliar DataFrame
            fato_auxiliar_df = self.build_fato_auxiliar(main_df)
            
            # Step 6: Save both DataFrames to Postgres
            success = True
            
            # Save meta_analitico with robust fallback
            if meta_analitico_df is not None:
                self.logger.info("Saving meta_analitico for eproc2g...")
                saved_ok = False
                try:
                    cod_indicadores = meta_analitico_df.select("cod_indicador").distinct().rdd.flatMap(lambda x: x).collect()
                    delete_cond = f"meta_ano = 2026 and cod_sistema = 2 and cod_indicador in ({', '.join(map(str, cod_indicadores))})"
                    self.logger.info(f"Will delete existing rows before inserting (eproc meta): {delete_cond}")
                    saved_ok = save_df_to_postgres_no_jar(meta_analitico_df, schema="dw2inst", table_name="meta_analitico", mode="append", delete_condition=delete_cond)
                except Exception as e:
                    saved_ok = False
                    self.logger.error(f"save_parquet_to_postgres raised exception for meta_analitico: {e}")
                    import traceback
                    self.logger.debug(traceback.format_exc())

                if not saved_ok:
                    # Diagnostic: persist a local parquet snapshot for inspection
                    try:
                        diagnostic_path = f"/tmp/diagnostic_meta_analitico_{int(time.time())}.parquet"
                        meta_analitico_df.write.mode("overwrite").parquet(diagnostic_path)
                        self.logger.warning(f"⚠️ Persisted diagnostic parquet for meta_analitico at: {diagnostic_path}")
                    except Exception as de:
                        self.logger.warning(f"⚠️ Could not persist diagnostic parquet: {de}")

                    # Try JDBC fallback that can create table via Spark if needed
                    try:
                        from utils.save_to_postgres import save_df_to_postgres
                        self.logger.info("Attempting JDBC fallback save for meta_analitico via save_df_to_postgres()")
                        # Use overwrite=False to append to existing table
                        save_df_to_postgres(meta_analitico_df, schema="dw2inst", table_name="meta_analitico", mode_user="append")
                        saved_ok = True
                        self.logger.info("✅ JDBC fallback save succeeded for meta_analitico")
                    except Exception as jf:
                        self.logger.error(f"❌ JDBC fallback save failed for meta_analitico: {jf}")
                        self.logger.debug(traceback.format_exc())
                        saved_ok = False

                success &= bool(saved_ok)

                
            else:
                self.logger.error("meta_analitico DataFrame is None")
                success = False
            
            # Save fato_auxiliar
            if fato_auxiliar_df is not None:
                self.logger.info("Saving fato_auxiliar for eproc2g...")
                success &= save_parquet_to_postgres(fato_auxiliar_df, schema="dw2inst", table_name="fato_auxiliar", mode="append")
                    
            else:
                self.logger.error("fato_auxiliar DataFrame is None")
                success = False
            
            if success:
                self.logger.info("eproc2g ETL pipeline completed successfully")
                return True
            else:
                self.logger.error("eproc2g ETL pipeline failed during save")
                return False
                
        except Exception as e:
            import traceback
            self.logger.error(f"eproc2g ETL pipeline failed: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def enrich_with_competencia_and_natureza(self, main_df):
        """
        Enrich main_df with cod_competencia and cod_natureza from eproc2g bronze tables
        """
        from pyspark.sql.functions import col, when, lit, row_number, regexp_replace, trim, coalesce as _coalesce
        from pyspark.sql.types import IntegerType
        from utils.parquet_utils import get_bronze_path, eproc2g_historico_distribuicao_judicial

        try:
            # Read eproc2g.processo from bronze using proper path function
            processo_path = get_bronze_path("eproc2g", "processo")
            df = main_df.drop("cod_competencia", "id_sigilo", "id_assunto_principal", "num_processo", "ind_segredo_justica", "id_classe_judicial")
            processo_df = self.spark.read.parquet(processo_path).select("id_processo", "cod_competencia", "id_sigilo", "id_assunto_principal", "num_processo", "id_classe_judicial")
            processo_df = processo_df.withColumnRenamed("id_processo", "id_processo_processo")
            
            # Join to get cod_competencia (left join to preserve all main_df records)
            df = df.join(processo_df, df["id_processo"] == processo_df["id_processo_processo"], how="inner").drop("id_processo_processo")
            self.logger.info("Joined with processo for cod_competencia")
        
            # Read eproc2g.competencia_judicial from bronze using proper path function
            comp_path = get_bronze_path("eproc2g", "competencia_judicial")
            self.logger.info(f"Reading competencia_judicial from: {comp_path}")
            comp_df = self.spark.read.parquet(comp_path).select("cod_competencia", "sin_civel", "sin_criminal")
            
            # Join to get natureza info
            df = df.join(comp_df, on="cod_competencia", how="left")
            
            # Set cod_natureza: 1=civil, 2=criminal (following your mapping)
            df = df.withColumn(
                "cod_natureza",
                when(col("sin_civel") == "S", lit(1))
                .when(col("sin_criminal") == "S", lit(2))
                .otherwise(lit(None))
            )
            
            # Drop helper columns
            df = df.drop("sin_civel", "sin_criminal")
            self.logger.info("Added cod_natureza successfully")
        
            # 2. Read eproc2g.log_processo for primary (first) values  
            log_processo_path = get_bronze_path("eproc2g", "log_processo")
            self.logger.info(f"Reading eproc2g.log_processo from: {log_processo_path}")
            log_processo_df = self.spark.read.parquet(log_processo_path)
            
            # Filter to only processes we need
            log_filtered = log_processo_df.join(df.select("id_processo").distinct(), on="id_processo", how="inner")
            
            # Get the FIRST (earliest) log entry per id_processo based on dth_alteracao
            window_first = Window.partitionBy("id_processo").orderBy(col("dth_alteracao").asc())
            
            primary_df = (log_filtered
                .withColumn("rn_first", row_number().over(window_first))
                .filter((col("rn_first") == 1) & (col("id_orgao_colegiado").isNotNull()))
                .select(
                    col("id_processo"),
                    col("id_orgao_colegiado").alias("cod_prim_oj")
                )
                .drop("rn_first")
            ).distinct()

            # Get the LAST (last) log entry per id_processo based on dth_alteracao
            window_last = Window.partitionBy("id_processo").orderBy(col("dth_alteracao").desc())
            
            ultimate_df = (log_filtered
                .withColumn("rn_last", row_number().over(window_last))
                .filter(col("rn_last") == 1)
                .select(
                    col("id_processo"),
                    col("id_orgao_colegiado").alias("cod_ult_oj")
                )
                .drop("rn_last")
            ).distinct()
            
            # 3. Join ultimate and primary data to fato_df
            df = df.join(ultimate_df.select("id_processo", "cod_ult_oj"), 
                                    on="id_processo", how="left")
            
            df = df.join(primary_df.select("id_processo", "cod_prim_oj"), 
                                    on="id_processo", how="left")
        
            historic_distr_path = eproc2g_historico_distribuicao_judicial()
            self.logger.info(f"Reading historico_distribuicao for mapping from: {historic_distr_path}")
            hist_raw = self.spark.read.parquet(historic_distr_path).select("cod_magistrado", "id_processo", "dth_distribuicao")
            w_hist_first = Window.partitionBy("id_processo").orderBy(col("dth_distribuicao").asc())
            w_hist_last = Window.partitionBy("id_processo").orderBy(col("dth_distribuicao").desc())

            hist_raw_first = hist_raw.withColumn("rn_first", row_number().over(w_hist_first)).filter(col("rn_first") == 1).drop("rn_first")
            hist_raw_first = hist_raw_first.withColumnRenamed("cod_magistrado", "cod_prim_magistrado").withColumnRenamed("dth_distribuicao", "dth_prim_distribuicao").withColumnRenamed("id_processo", "id_processo_hist_prim")            
            
            hist_raw_last = hist_raw.withColumn("rn_last", row_number().over(w_hist_last)).filter(col("rn_last") == 1).drop("rn_last")
            hist_raw_last = hist_raw_last.withColumnRenamed("cod_magistrado", "cod_ult_magistrado").withColumnRenamed("dth_distribuicao", "dth_ult_distribuicao").withColumnRenamed("id_processo", "id_processo_hist_ult")

            df = df.join(hist_raw_first.select("id_processo_hist_prim", "cod_prim_magistrado", "dth_prim_distribuicao"), 
                                    df["id_processo"] == hist_raw_first["id_processo_hist_prim"], how="left").drop("id_processo_hist_prim")

            df = df.join(hist_raw_last.select("id_processo_hist_ult", "cod_ult_magistrado", "dth_ult_distribuicao"), 
                                    df["id_processo"] == hist_raw_last["id_processo_hist_ult"], how="left").drop("id_processo_hist_ult")
            self.logger.info("Added cod_prim_oj, cod_ult_oj, cod_prim_magistrado, cod_ult_magistrado successfully")

            # Set cod_sistema to 2 (eproc2g system)
            df = df.withColumn("cod_sistema", lit(2))
            return df   
            
        except Exception as e:
            self.logger.warning(f"Failed to join ultimate and primary data: {e}")
            return None

        

    def enrich_with_assunto_judicial(self, main_df):
        """
        Enrich main_df with cod_mni_assunto_judicial from assunto_judicial table
        """
        from pyspark.sql.functions import col, lit
        from utils.parquet_utils import get_bronze_path
        try:
            # Read eproc2g.assunto_judicial from bronze
            assunto_path = get_bronze_path("eproc2g", "assunto_judicial")
            self.logger.info(f"Reading assunto_judicial from: {assunto_path}")
            assunto_df = self.spark.read.parquet(assunto_path).select("id_assunto_judicial", "cod_mni_assunto_judicial")
            
            # Join to get cod_mni_assunto_judicial (left join to preserve all main_df records)
            df = main_df.join(assunto_df, 
                            main_df["id_assunto_principal"] == assunto_df["id_assunto_judicial"], 
                            how="inner")
            
            # Drop the join column we don't need
            df = df.drop("id_assunto_judicial", "id_assunto_principal")
            self.logger.info("Enriched with cod_mni_assunto_judicial from assunto_judicial")
            return df
            
        except Exception as e:
            self.logger.error(f"Error enriching with assunto_judicial: {e}")
            # If assunto_judicial read fails, add cod_mni_assunto_judicial as null
            df = main_df.withColumn("cod_mni_assunto_judicial", lit(None))
            return df

    def build_meta_analitico(self, main_df):
        """
        Build meta_analitico DataFrame for eproc2g with required columns using EJUD-style cod_indicador mapping.
        Schema: id_processo, num_processo, cod_sistema, cod_classe_cnj, id_assunto_principal, 
                cod_competencia, dth_evento_indicador, cod_indicador, id_sigilo, cod_natureza, dth_atualizacao
        """
        from pyspark.sql.functions import col, lit, current_timestamp, coalesce, regexp_replace, when, lower
        try:
            self.logger.info("Building meta_analitico for eproc2g...")
            # Check EPROC-specific columns availability
            eproc_expected_cols = ["num_processo", "id_classe_judicial", "cod_mni_assunto_judicial", 
                                 "dth_inicio", "dth_fim", "dth_referencia", "meta_indicator", 
                                 "variavel", "id_sigilo", "cod_mni_movimento", "id_orgao_colegiado", "cod_ult_oj", "cod_ult_magistrado"]
            missing_cols = [col_name for col_name in eproc_expected_cols if col_name not in main_df.columns]
            if missing_cols:
                self.logger.warning(f"Missing expected EPROC columns: {missing_cols}")
                
            meta_df = main_df
            # CRITICAL: Create variavel column from source_variable for cod_indicador mapping
            if "source_variable" in meta_df.columns:
                meta_df = meta_df.withColumn("variavel", col("source_variable"))
                self.logger.info("Created variavel column from source_variable for cod_indicador mapping")
            else:
                self.logger.warning("source_variable column not found - cod_indicador mapping may fail")
            
            # Map eproc2g columns to meta_analitico schema (exact column order) - UPDATED to include cod_ult_oj and cod_ult_magistrado
            meta_df = meta_df.select(
                col("id_processo"),
                col("num_processo") if "num_processo" in meta_df.columns else lit(None).alias("num_processo"),
                col("cod_sistema"),  
                col("id_classe_judicial").alias("cod_classe_cnj") if "id_classe_judicial" in meta_df.columns else lit(None).alias("cod_classe_cnj"),
                col("cod_mni_assunto_judicial").alias("id_assunto_principal") if "cod_mni_assunto_judicial" in meta_df.columns else lit(None).alias("id_assunto_principal"),
                col("cod_competencia") if "cod_competencia" in meta_df.columns else lit(None).alias("cod_competencia"),
                # For EPROC: prioritize dth_referencia (most accurate), then dth_inicio, then dth_fim
                coalesce(col("dth_inicio"), col("PendJulg01TG_dth_inicio")).alias("dth_evento_indicador"),
                col("cod_indicador") if "cod_indicador" in meta_df.columns else lit(None).alias("cod_indicador"),
                col("id_sigilo") if "id_sigilo" in meta_df.columns else lit(None).alias("id_sigilo"),
                col("cod_natureza") if "cod_natureza" in meta_df.columns else lit(None).alias("cod_natureza"),
                col("variavel") if "variavel" in meta_df.columns else lit(None).alias("variavel"),
                current_timestamp().alias("dth_atualizacao"),
                col("cod_ult_oj") if "cod_ult_oj" in meta_df.columns else lit(None).alias("cod_ult_oj"),
                col("cod_ult_magistrado") if "cod_ult_magistrado" in meta_df.columns else lit(None).alias("cod_ult_magistrado")
            )
            
            # FIX 1: Pad num_processo with leading zeros to make it 20 digits
            from pyspark.sql.functions import lpad
            if "num_processo" in meta_df.columns:
                meta_df = meta_df.withColumn("num_processo", lpad(col("num_processo"), 20, "0"))
                self.logger.info("✅ Applied num_processo padding to 20 digits")
            
            # FIX 2: cod_indicador should already be mapped before union
            self.logger.info("Checking cod_indicador mapping (should be done already)...")
            
            # Add meta_ano
            target_year = 2026
            meta_df = meta_df.withColumn("meta_ano", lit(target_year))

            # Cast integer columns to remove decimal places before final selection (now includes cod_ult_oj and cod_ult_magistrado)
            integer_columns = ["cod_sistema", "cod_classe_cnj", "id_assunto_principal", "cod_competencia", 
                             "cod_indicador", "id_sigilo", "cod_natureza", "cod_ult_oj", "cod_ult_magistrado"]
            
            for int_col in integer_columns:
                if int_col in meta_df.columns:
                    meta_df = meta_df.withColumn(int_col, col(int_col).cast("Integer"))

            # Ensure final column order matches exact schema - UPDATED to include cod_ult_oj, cod_ult_magistrado and meta_ano at the end
            final_columns = [
                "id_processo", "num_processo", "cod_sistema", "cod_classe_cnj", "id_assunto_principal",
                "cod_competencia", "dth_evento_indicador", "cod_indicador", "id_sigilo", "cod_natureza", 
                "dth_atualizacao", "cod_ult_oj", "cod_ult_magistrado", "meta_ano"
            ]
            
            meta_df = meta_df.select(*final_columns)
            
            self.logger.info(f"Built meta_analitico with columns: {meta_df.columns}")
            
            return meta_df
            
        except Exception as e:
            self.logger.error(f"Error building meta_analitico: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None

    def build_fato_auxiliar(self, main_df):
        """
        Build fato_auxiliar DataFrame for eproc2g with all required joins and mappings.
        Schema: id_processo, dth_prim_distribuicao, dth_ult_distribuicao, cod_prim_oj, cod_ult_oj, 
                cod_prim_magistrado, cod_ult_magistrado, dth_ult_evento, cod_ult_evento_cnj, 
                ind_baixado, ind_julgado, id_sigilo
        """
        from pyspark.sql.functions import col, lit, when, row_number, first
        from pyspark.sql.window import Window
        try:
            from utils.parquet_utils import get_bronze_path
            
            self.logger.info("Building fato_auxiliar for eproc2g...")            
            # Start with the unique processo base (one row per id_processo)
            fato_df = main_df
            # Add event data (simplified approach)
            try:
                df_processo_evento = get_bronze_path("eproc2g", "processo_evento")
                df_evento_judicial = get_bronze_path("eproc2g", "evento_judicial")

                proc_even_window = Window.partitionBy("id_processo").orderBy(col("dth_evento").desc())
                evento_df = (self.spark.read.parquet(df_processo_evento).select("id_processo", "id_evento_judicial", "dth_evento"))
                evento_df = evento_df.withColumn("rn", row_number().over(proc_even_window )).filter(col("rn") == 1).drop("rn")
                evento_df = evento_df.withColumnRenamed("id_processo", "id_processo_evento").withColumnRenamed("dth_evento", "dth_ult_evento")

                evento_jud_df = self.spark.read.parquet(df_evento_judicial).select("id_evento_judicial", "cod_mni_movimento")
                evento_jud_df = evento_jud_df.withColumnRenamed("id_evento_judicial", "id_evento_judicial_jud").withColumnRenamed("cod_mni_movimento", "cod_ult_evento_cnj")

                evento_df = evento_df.join(evento_jud_df, evento_df["id_evento_judicial"] == evento_jud_df["id_evento_judicial_jud"], how="left").drop("id_evento_judicial").drop("id_evento_judicial_jud")
                
                fato_df = fato_df.join(evento_df, fato_df["id_processo"] == evento_df["id_processo_evento"], how="left").drop("id_processo_evento")
                
            except Exception as e:
                self.logger.error(f"❌ Error building event data: {e}")
                fato_df = fato_df.withColumn("dth_ult_evento", lit(None)) \
                               .withColumn("cod_ult_evento_cnj", lit(None))
            
            # Get ind_baixado from baixm01.parquet
            try:
                from utils.parquet_utils import parquet_path
                baixado_path = parquet_path("data/prata/eproc2g/meta_cnj2026/meta_01/current/baixm01.parquet")
                self.logger.info(f"📊 Reading ind_baixado from: {baixado_path}")
                baixado_df = self.spark.read.parquet(baixado_path).select("id_processo").withColumn("ind_baixado", lit(1))
                
                fato_df = fato_df.join(baixado_df, on="id_processo", how="left")
                fato_df = fato_df.withColumn("ind_baixado", when(col("ind_baixado") == 1, lit(1)).otherwise(lit(0)))
                self.logger.info("✅ Added ind_baixado successfully")
            except Exception as e:
                self.logger.error(f"❌ Error getting ind_baixado: {e}")
                fato_df = fato_df.withColumn("ind_baixado", lit(0))
            
            # Get ind_julgado from julgadom01.parquet
            try:
                from utils.parquet_utils import parquet_path
                julgado_path = parquet_path("data/prata/eproc2g/meta_cnj2026/meta_01/current/julgadom01.parquet")
                self.logger.info(f"📊 Reading ind_julgado from: {julgado_path}")
                julgado_df = self.spark.read.parquet(julgado_path).select("id_processo").withColumn("ind_julgado", lit(1))
                
                fato_df = fato_df.join(julgado_df, on="id_processo", how="left")
                fato_df = fato_df.withColumn("ind_julgado", when(col("ind_julgado") == 1, lit(1)).otherwise(lit(0)))
                self.logger.info("✅ Added ind_julgado successfully")
            except Exception as e:
                self.logger.error(f"❌ Error getting ind_julgado: {e}")
                fato_df = fato_df.withColumn("ind_julgado", lit(0))
            
            # Cast integer columns to remove decimal places before final selection
            integer_columns = ["cod_prim_oj", "cod_ult_oj", "cod_prim_magistrado", "cod_ult_magistrado",
                             "cod_ult_evento_cnj", "ind_baixado", "ind_julgado", "id_sigilo"]

            for int_col in integer_columns:
                if int_col in fato_df.columns:
                    fato_df = fato_df.withColumn(int_col, col(int_col).cast("Integer"))

            # Colunas obrigatórias da fato_auxiliar ausentes na prata eproc2g
            fato_df = fato_df.withColumn("complemento", lit(None).cast("string"))
            fato_df = fato_df.withColumn("meta_ano",    lit(2026).cast("int"))

            # Select final columns em ordem exata do schema
            final_columns = [
                "id_processo", "dth_prim_distribuicao", "dth_ult_distribuicao", "cod_prim_oj", "cod_ult_oj",
                "cod_prim_magistrado", "cod_ult_magistrado", "dth_ult_evento", "cod_ult_evento_cnj",
                "ind_baixado", "ind_julgado", "id_sigilo", "complemento", "meta_ano",
            ]
            fato_df = fato_df.select(*[col(c) if c in fato_df.columns else lit(None).alias(c) for c in final_columns])

            # CRITICAL: Ensure one row per id_processo (like EJUD facto_auxiliar.py does)
            fato_df = fato_df.filter(col("id_processo").isNotNull()).dropDuplicates(["id_processo"])
            
            return fato_df
        except Exception as e:
            self.logger.error(f"Error building fato_auxiliar: {e}")
            return None