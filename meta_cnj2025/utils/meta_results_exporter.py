"""
OPTIMIZED MetaResultsExporter - Data Engineering Best Practices
Fixes memory issues, inefficient joins, and configuration coupling
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, broadcast, coalesce, regexp_replace, trim, when
from pyspark.sql.types import *
import logging
from datetime import datetime
from pathlib import Path
import os
import pandas as pd
from typing import Union, List
import json

logger = logging.getLogger(__name__)

class MetaResultsExporter:
    """
    OPTIMIZED Meta Results Exporter with memory-efficient operations
    and proper Spark DataFrame handling for production environments.
    """
    
    # Required columns for meta exports - standardized schema
    REQUIRED_META_COLUMNS = [
        "cod_classe", "cod_oj", "cod_gab", "cod_proc", "cod_tip_mov",
        "cod_tip_mov_numero", "coddoc", "dth_fim", "dthrmov", "ident_mov",
        "nivel_sigilo", "situacao", "variavel", "ano", "mes", "origin_data"
    ]

    def __init__(self, session, meta_number: str, base_path: str = None, config_manager=None):
        """Initialize with proper Spark session management."""
        self.spark = session  # Use provided session consistently
        self.meta_number = meta_number.zfill(2)
        self.config_manager = config_manager
        
        # Path resolution with fallbacks
        self._resolve_export_paths(base_path)
        self._ensure_directories()
        
        # Session tracking
        self.session_start = datetime.now()
        
        logger.info(f"🚀 Optimized MetaResultsExporter initialized for Meta {self.meta_number}")
        logger.info(f"📁 Export path: {self.meta_path}")

    def _resolve_export_paths(self, base_path):
        """Resolve export paths with proper fallback strategy."""
        try:
            if self.config_manager:
                meta_key = f"meta_{self.meta_number}"
                configured_path = self.config_manager.get_export_path(meta_key)
                if configured_path:
                    self.meta_path = configured_path
                    self.base_path = configured_path.rsplit('/', 1)[0]
                    logger.info(f"✅ Using configured export path: {self.meta_path}")
                    return
        except Exception as e:
            logger.warning(f"⚠️ Could not get configured path: {e}")
        
        # Fallback to provided or default path
        self.base_path = base_path or "/opt/parquet/data/prata/ejud"
        self.meta_path = f"{self.base_path}/ejud_meta2025_meta{self.meta_number}"

    def _ensure_directories(self):
        """Create necessary directories efficiently."""
        try:
            subdirs = ["current", "archive", "metadata"]
            for subdir in subdirs:
                Path(f"{self.meta_path}/{subdir}").mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Directories ensured for Meta {self.meta_number}")
        except Exception as e:
            logger.error(f"❌ Failed to create directories: {e}")
            raise
    
    
    def export_results(self, 
                      data: Union[DataFrame, pd.DataFrame, dict], 
                      variable_name: str,
                      include_timestamp: bool = True,
                      archive_previous: bool = True,
                      optimize_partitions: bool = True,
                      incremental_mode: bool = False) -> bool:
        """
        OPTIMIZED export with proper memory management and partitioning.
        """
        from pyspark.sql.functions import current_timestamp, lit 
        try:
            start_time = datetime.now()
            
            if data is None:
                logger.warning(f"⚠️ No data provided for {variable_name}")
                return False

            # Convert to Spark DataFrame efficiently
            spark_df = self._convert_to_spark_df(data, variable_name)
            if spark_df is None:
                return False

            # Define paths
            current_path = f"{self.meta_path}/current/{variable_name}.parquet"
            archive_path = f"{self.meta_path}/archive/{variable_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            temp_path = f"{self.meta_path}/current/_temp_{variable_name}.parquet"

            # Step 1: Write raw data to temp parquet to break DAG lineage.
            # The incoming DF carries a complex plan from joins/windows/unions.
            # Writing + reading back is the simplest way to get a clean plan
            # without using localCheckpoint (which fills temp disk).
            spark_df.write.mode("overwrite").parquet(temp_path)
            logger.info(f"📊 {variable_name} written to temp parquet (lineage broken)")

            # Step 2: Read back with clean lineage, apply schema, and write final
            spark_df = self.spark.read.parquet(temp_path)

            # Enforce schema and add metadata
            spark_df = self._enforce_meta_output_schema(spark_df, variable_name)

            if include_timestamp:
                spark_df = (spark_df
                           .withColumn("export_timestamp", current_timestamp())
                           .withColumn("meta_number", lit(f"meta_{self.meta_number}"))
                           .withColumn("variable_name", lit(variable_name)))

            # Fix void columns
            spark_df = self._handle_void_columns(spark_df)

            # Archive previous if needed
            if archive_previous and os.path.exists(current_path):
                try:
                    os.rename(current_path, archive_path)
                    logger.info(f"📦 Archived previous {variable_name}")
                except Exception as e:
                    logger.warning(f"⚠️ Archive failed: {e}")

            # Write final output
            spark_df.write.mode("overwrite").parquet(current_path)
            logger.info(f"✅ Exported {variable_name}")
            self._save_export_metadata(variable_name, current_path)

            # Cleanup temp file
            try:
                import shutil
                shutil.rmtree(temp_path, ignore_errors=True)
            except Exception:
                pass
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Export failed for {variable_name}: {e}")
            return False
      
    def _convert_to_spark_df(self, data: Union[DataFrame, pd.DataFrame, dict], variable_name: str) -> DataFrame:
        """Convert various data types to Spark DataFrame efficiently."""
        try:
            if isinstance(data, dict):
                rows = [{"key": k, "value": str(v), "variable": variable_name} for k, v in data.items()]
                return self.spark.createDataFrame(rows)
            elif hasattr(data, 'toPandas'):  # Already Spark DataFrame
                return data
            elif isinstance(data, pd.DataFrame):
                return self.spark.createDataFrame(data)
            else:
                logger.error(f"❌ Unsupported data type: {type(data)}")
                return None
        except Exception as e:
            logger.error(f"Failed to convert data to Spark DataFrame: {e}")
            return None

    def _enforce_meta_output_schema(self, spark_df: DataFrame, variable_name: str):
        """Enforce standardized meta output schema efficiently with EPROC compatibility."""
        from pyspark.sql.functions import lit
        # Apply EPROC to EJUD schema mapping if needed
        spark_df = self._apply_eproc_schema_mapping(spark_df)

        # Default types mapping
        default_types = {
            "cod_classe": StringType(), "cod_oj": StringType(), "cod_gab": StringType(),
            "cod_proc": StringType(), "cod_tip_mov": IntegerType(), "cod_tip_mov_numero": IntegerType(),
            "coddoc": LongType(), "dth_fim": TimestampType(), "dthrmov": TimestampType(),
            "ident_mov": StringType(), "nivel_sigilo": IntegerType(), "situacao": IntegerType(),
            "variavel": StringType(), "ano": IntegerType(), "mes": IntegerType(),
            "origin_data": StringType()
        }


    # Coerce existing columns to expected types when possible to avoid Parquet read/write schema mismatches.
    # This addresses errors such as: physicalType: BINARY, logicalType: bigint

        for col_name in self.REQUIRED_META_COLUMNS:
            expected_type = default_types.get(col_name)
            if col_name in spark_df.columns:
                # If present but type differs, attempt safe coercion
                try:
                    field = next((f for f in spark_df.schema.fields if f.name == col_name), None)
                    if field is not None and type(field.dataType) is not type(expected_type):
                        # Use DEBUG here to avoid noisy INFO spam during normal runs.
                        logger.debug(f"ℹ️ Column '{col_name}' has type {field.dataType} but expected {expected_type}; attempting safe cast")

                        # If expected is numeric, first strip non-digits then cast
                        if isinstance(expected_type, (LongType, IntegerType)):
                            # Remove any non-digit characters (safe for IDs) and trim
                            cleaned = regexp_replace(col(col_name).cast("string"), r'[^0-9]', "")
                            cleaned = trim(cleaned)

                            # Map expected_type to cast string
                            if isinstance(expected_type, LongType):
                                cast_tgt = "long"
                            else:
                                cast_tgt = "int"

                            spark_df = spark_df.withColumn(
                                col_name,
                                when(cleaned == "", lit(None)).otherwise(cleaned.cast(cast_tgt))
                            )
                        else:
                            # Fallback: try direct cast, otherwise cast via string
                            spark_df = spark_df.withColumn(
                                col_name,
                                coalesce(col(col_name).cast(expected_type), col(col_name).cast(StringType()).cast(expected_type))
                            )
                except Exception as e:
                    # Coercion failures are noteworthy but not fatal; surface as WARNING
                    logger.warning(f"⚠️ Could not coerce column {col_name}: {e}")
            else:
                # Add missing column with expected type
                spark_df = spark_df.withColumn(col_name, lit(None).cast(expected_type))

        # Set variable name (override if present)
        spark_df = spark_df.withColumn("variavel", lit(variable_name).cast(StringType()))

        # Select and order columns
        return spark_df.select(self.REQUIRED_META_COLUMNS)

    def _apply_eproc_schema_mapping(self, spark_df: DataFrame) -> DataFrame:
        """
        Apply EPROC to EJUD schema mapping for cross-system compatibility.
        
        Maps EPROC column names to EJUD expected schema to ensure proper
        datetime field handling and downstream compatibility.
        
        Args:
            spark_df: DataFrame to transform
            
        Returns:
            DataFrame: Transformed DataFrame with EJUD-compatible schema
        """
        from pyspark.sql.functions import col, lit
        from pyspark.sql.types import LongType, IntegerType
        
        try:
            # CRITICAL: Map dth_inicio (EPROC) -> dthrmov (EJUD) for datetime events
            if "dth_inicio" in spark_df.columns and "dthrmov" not in spark_df.columns:
                spark_df = spark_df.withColumn("dthrmov", col("dth_inicio"))
                logger.info("🔄 MetaResultsExporter: Mapped EPROC dth_inicio -> dthrmov")
            
            # Map id_processo (EPROC) -> coddoc (EJUD) for process identification
            if "id_processo" in spark_df.columns and "coddoc" not in spark_df.columns:
                spark_df = spark_df.withColumn("coddoc", col("id_processo").cast(LongType()))
                logger.info("🔄 MetaResultsExporter: Mapped EPROC id_processo -> coddoc")
            
            # Map id_sigilo (EPROC) -> nivel_sigilo (EJUD) for privacy level
            if "id_sigilo" in spark_df.columns and "nivel_sigilo" not in spark_df.columns:
                spark_df = spark_df.withColumn("nivel_sigilo", col("id_sigilo").cast(IntegerType()))
                logger.info("🔄 MetaResultsExporter: Mapped EPROC id_sigilo -> nivel_sigilo")
            
            # Map cod_mni_movimento (EPROC) -> cod_tip_mov (EJUD) for movement type  
            if "cod_mni_movimento" in spark_df.columns and "cod_tip_mov" not in spark_df.columns:
                spark_df = spark_df.withColumn("cod_tip_mov", col("cod_mni_movimento"))
                logger.info("🔄 MetaResultsExporter: Mapped EPROC cod_mni_movimento -> cod_tip_mov")
            
            return spark_df
            
        except Exception as e:
            logger.warning(f"⚠️ MetaResultsExporter schema mapping failed: {e}")
            return spark_df

    def _handle_void_columns(self, spark_df: DataFrame) -> DataFrame:
        """Fix void columns for Parquet compatibility."""
        from pyspark.sql.functions import lit
        from pyspark.sql.types import StringType
        
        try:
            void_columns = [field.name for field in spark_df.schema.fields 
                          if field.dataType.typeName() == 'void']
            
            if void_columns:
                logger.info(f"🔧 Fixing {len(void_columns)} void columns")
                for col_name in void_columns:
                    spark_df = spark_df.withColumn(col_name, lit(None).cast(StringType()))
            
            return spark_df
        except Exception as e:
            logger.warning(f"⚠️ Void column fix failed: {e}")
            return spark_df

    def _get_meta_relevant_codass(self, meta_num: str) -> List[int]:
        """Get relevant codass values for meta with proper error handling."""
        try:
            config_dir = Path(__file__).parent.parent / "config"
            
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
            logger.info(f"Meta {meta_num}: Found {len(result)} relevant codass values")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get relevant codass for meta {meta_num}: {e}")
            return []

    def _save_export_metadata(self, variable_name: str, file_path: str):
        """Save export metadata efficiently."""
        try:
            metadata = {
                "variable_name": variable_name,
                "export_timestamp": datetime.now().isoformat(),
                "file_path": file_path,
                "meta_number": self.meta_number,
                "file_size_mb": self._get_file_size_mb(file_path)
            }
            
            metadata_path = f"{self.meta_path}/metadata/{variable_name}_metadata.json"
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
                
        except Exception as e:
            logger.warning(f"⚠️ Failed to save metadata: {e}")

    def _get_file_size_mb(self, file_path: str) -> float:
        """Get file size in MB efficiently."""
        try:
            if os.path.exists(file_path):
                size_bytes = sum(
                    os.path.getsize(os.path.join(dirpath, filename))
                    for dirpath, dirnames, filenames in os.walk(file_path)
                    for filename in filenames
                )
                return round(size_bytes / (1024 * 1024), 2)
            return 0.0
        except:
            return 0.0

    def get_session_summary(self) -> dict:
        """Get export session summary for monitoring."""
        duration = (datetime.now() - self.session_start).total_seconds()
        
        return {
            "meta_number": self.meta_number,
            "total_records_exported": self.total_records_exported,
            "session_duration_seconds": duration,
            "avg_records_per_second": self.total_records_exported / max(duration, 1)
        }
