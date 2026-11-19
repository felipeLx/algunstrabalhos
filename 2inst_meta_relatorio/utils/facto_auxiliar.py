from .parquet_utils import get_silver_path, ejud_bronze_tab_xml, ejud_bronze_movimentorelator
import os
from dotenv import load_dotenv
# Import the safe python writer
from .save_to_postgres import save_df_to_postgres_no_jar 

load_dotenv()

class FactoAuxiliar:
    def __init__(self, df, spark, logger, meta_number="01", mode="append"):
        self.spark = spark
        self.logger = logger
        self.meta_number = meta_number
        self.mode = mode
        self.df = df
        # This will be populated in build_and_save by reading Phase 1 output
        self._filter_df = None 
        self.logger.info(f"FactoAuxiliar initialized for Meta {meta_number}")

    def build_and_save(self):
        from pyspark.sql.functions import coalesce, lit, trim, regexp_replace, col, when, row_number
        from pyspark.sql.window import Window
        self.logger.info(f"🏗️ Starting FactoAuxiliar Build for Meta {self.meta_number}")

        # Read both tab_xml_andamento and movimentorelator and prefer rows where cod_tip_mov is not null
        tab_path = ejud_bronze_tab_xml()
        rel_path = ejud_bronze_movimentorelator()

        tab_df = self.spark.read.parquet(tab_path) \
            .join(self.df.select("id_processo").distinct(), col("coddoc") == col("id_processo"), "semi") \
            .select(col("coddoc"), col("dthrmov"), col("cod_tip_mov"), col("complemento"))

        try:
            rel_df = self.spark.read.parquet(rel_path) \
                .join(self.df.select("id_processo").distinct(), col("coddoc") == col("id_processo"), "semi") \
                .select(col("coddoc"), col("dthrmov"), col("cod_tip_mov"), col("complemento"))
        except Exception:
            rel_df = self.spark.createDataFrame([], schema=tab_df.schema)

        # Union both sources (allowMissingColumns not needed if schemas match)
        movimento_df = tab_df.unionByName(rel_df, allowMissingColumns=True)

        # Prioritize rows with non-null cod_tip_mov and latest dthrmov
        evtWindow = Window.partitionBy("coddoc").orderBy((col("cod_tip_mov").isNotNull()).desc(), col("dthrmov").desc())
        evt_ranked = movimento_df.withColumn("rn_evt", row_number().over(evtWindow)).filter(col("rn_evt") == 1).drop("rn_evt")
        evt_map = evt_ranked.select(col("coddoc").alias("coddoc_movimento"), col("dthrmov").alias("dth_ult_evento"), col("cod_tip_mov").alias("cod_ult_evento_cnj").cast("integer"), col("complemento"))
        # --- Step 2: Join last event data to the main DataFrame ---
        # Defensive: ensure id_processo and movement keys are strings for stable join
        if "id_processo" in self.df.columns:
            base_df = self.df.withColumn("id_processo", col("id_processo").cast("string"))
        else:
            base_df = self.df
        evt_map = evt_map.withColumn("coddoc_movimento", col("coddoc_movimento").cast("string"))
        final = base_df.join(evt_map, base_df["id_processo"] == evt_map["coddoc_movimento"], how="left").drop("coddoc_movimento")
        self.logger.info("Successfully joined with movimento data.")

        baixa_julgad_df = self._get_aux_indicadores_df()
        #_filter_df.unpersist() # No longer need the filter DF

        if baixa_julgad_df is not None:
            # ensure coddoc is string
            baixa_julgad_df = baixa_julgad_df.withColumn("coddoc", col("coddoc").cast("string"))
            final = final.join(baixa_julgad_df, final["id_processo"] == baixa_julgad_df["coddoc"], how="left").drop("coddoc")
            # normalize indicator columns
            final = final.withColumn("ind_baixado", coalesce(col("ind_baixado"), lit(0)).cast("smallint"))
            final = final.withColumn("ind_julgado", coalesce(col("ind_julgado"), lit(0)).cast("smallint"))

        # Build a deterministic ordering key (prefer last event timestamp, then last distribution)
        order_col = coalesce(col("dth_ult_evento"), col("dth_ult_distribuicao"))

        # Select only the needed columns first to reduce shuffle
        needed_cols = [
            "id_processo",
            "dth_prim_distribuicao",
            "dth_ult_distribuicao",
            "cod_prim_oj",
            "cod_ult_oj",
            "cod_prim_magistrado",
            "cod_ult_magistrado",
            "dth_ult_evento",
            "cod_ult_evento_cnj",
            "ind_baixado",
            "ind_julgado",
            "id_sigilo",
            "complemento"
        ]

        proj = [col(c) if c in final.columns else lit(None).alias(c) for c in needed_cols]
        proj.append(order_col.alias("_order_ts"))

        reduced = final.select(*proj).filter(col("id_processo").isNotNull())

        # Repartition by key to colocate and reduce shuffle for the window
        reduced = reduced.repartition(col("id_processo"))

        # Deterministic dedupe: keep the row with the latest ordering timestamp
        win = Window.partitionBy("id_processo").orderBy(col("_order_ts").desc_nulls_last())
        ranked = reduced.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn", "_order_ts")

        # Trim string columns in a single select to avoid multiple passes
        string_cols = [c for c, t in ranked.dtypes if "string" in t.lower()]
        final_exprs = []
        for c in ranked.columns:
            if c in string_cols:
                final_exprs.append(trim(regexp_replace(col(c), r"\s+", " ")).alias(c))
            else:
                final_exprs.append(col(c))

        final = ranked.select(*final_exprs)

        # --- Trim all string columns and detect potential ';' delimiters introduced at read time ---
        try:
            string_cols = [c for c, t in final.dtypes if "string" in t.lower()]
            if string_cols:
                self.logger.info(f"Trimming {len(string_cols)} string columns to reduce unexpected large values")
                for c in string_cols:
                    final = final.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))
            
            target_year = 2026
            final = final.withColumn(
                "meta_ano",
                lit(target_year)
            )

            # O pre-pipeline DELETE em run_all_metas.py já limpou fato_auxiliar para meta_ano=target_year
            # antes do loop de metas. Fazemos append direto sem reconstruir o IN clause (caro + frágil).
            self.logger.info(f"fato_auxiliar (meta {self.meta_number}): append direto — pré-pipeline já limpou meta_ano={target_year}")

            save_df_to_postgres_no_jar(
                    final,
                    schema="dw2inst",
                    table_name="fato_auxiliar",
                    mode=self.mode,
                    delete_condition=None
                )
            return True
    
        except Exception as e:
            # If anything goes wrong (count/coalesce/write), attempt a conservative fallback write
            self.logger.warning(f"Primary write path failed (count/coalesce/write): {e}")
            return False

    def _get_aux_indicadores_df(self):
        from pyspark.sql.functions import coalesce, lit, trim, regexp_replace, col, when, row_number
        from pyspark.sql.window import Window

        try:
            base_path = get_silver_path("ejud", "meta_cnj2026")
            baix_path = os.path.join(base_path, f"meta_{self.meta_number}", "current", f"baixm{self.meta_number}.parquet")
            julgado_path = os.path.join(base_path, f"meta_{self.meta_number}", "current", f"julgadom{self.meta_number}.parquet")

            baixa_df = self.spark.read.parquet(baix_path).select(col("coddoc").cast("string").alias("coddoc"), lit(1).alias("ind_baixado"))
            julgado_df = self.spark.read.parquet(julgado_path).select(col("coddoc").cast("string").alias("coddoc"), lit(1).alias("ind_julgado"))

            if self._filter_df:
                # Using broadcast hint might speed this up if filter is small, but standard join is safer for large metas
                baixa_df = baixa_df.join(self._filter_df, baixa_df.coddoc == self._filter_df.id_processo, "inner").drop("id_processo")
                julgado_df = julgado_df.join(self._filter_df, julgado_df.coddoc == self._filter_df.id_processo, "inner").drop("id_processo")

            combined = baixa_df.join(julgado_df, "coddoc", "outer") \
                .select(
                    col("coddoc"), 
                    when(col("ind_baixado").isNotNull(), 1).otherwise(0).alias("ind_baixado"),
                    when(col("ind_julgado").isNotNull(), 1).otherwise(0).alias("ind_julgado")
                )

            return combined
        except Exception as e:
            self.logger.error(f"Error reading aux indicators: {e}")
            raise
