"""
Create filtered parquet files for EPROC eligible processes.

What it does (Spark path):
- Build base eligible processes for CONHECIMENTO classes
- Save a meta_01-friendly eligible list (id_processo) for EPROC
- For each assunto group (config/assunto_groups.json), intersect eligible
    processes with process-assunto and save id_processo lists (mirrors EJUD
    create_filtered_assunto_parquets, but using id_processo).

EPROC specifics:
- Uses id_processo (string)
- Reads mni_classe_judicial, relacao_classe_judicial_mni, processo from bronze
- Reads processoassunto/processo_assunto from bronze for assunto filters

Outputs:
- parquet_files/eproc/filtered_id_processo_classe_conhecimento.parquet
- parquet_files/eproc/meta_01/eligible_id_processo.parquet
- parquet_files/eproc/filtered_id_processo_<assunto>.parquet for each group
"""

from pathlib import Path
from typing import List, Optional, Tuple
import os
import json
import logging
import glob

# Setup logger
logger = logging.getLogger("create_filtered_eproc_parquets")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

SCRIPT_DIR = Path(__file__).resolve().parent
PARQUET_DIR = SCRIPT_DIR.parent / "parquet_files" / "eproc"
EXCEL_PATH = SCRIPT_DIR.parent / "csv" / "classes.xlsx"
# Try both spellings: assunto_groups.json (current) and assuntos_groups.json (legacy)
ASSUNTO_CONFIG_PATHS = [
    SCRIPT_DIR.parent / "config" / "assunto_groups.json",
    SCRIPT_DIR.parent / "config" / "assuntos_groups.json",
]

# EPROC bronze paths
EPROC_BRONZE_BASE = Path("/opt/parquet/data/bronze/eproc2g")
EPROC_ASSUNTO_CANDIDATES = [
    EPROC_BRONZE_BASE / "processoassunto",
    EPROC_BRONZE_BASE / "processo_assunto",
]


def load_conhecimento_codes_from_excel() -> List[str]:
    """
    Load CONHECIMENTO procedure codes from classes.xlsx file.

    Returns:
        List of codes where "Tipo de Procedimento" equals "CONHECIMENTO"
    """
    try:
        import pandas as pd

        logger.info(f"📖 Reading classes.xlsx from {EXCEL_PATH}")

        if not EXCEL_PATH.exists():
            logger.error(f"❌ Excel file not found: {EXCEL_PATH}")
            return []

        # Read Excel file
        df = pd.read_excel(EXCEL_PATH)
        logger.info(f"📊 Excel file loaded: {len(df)} rows, columns: {list(df.columns)}")

        # Filter for CONHECIMENTO (column index 2)
        conhecimento_df = df[df.iloc[:, 2] == "CONHECIMENTO"]
        logger.info(f"📊 CONHECIMENTO records found: {len(conhecimento_df)}")

        if len(conhecimento_df) == 0:
            logger.warning("⚠️ No CONHECIMENTO records found! Checking for similar terms...")
            # Try case-insensitive or partial matches
            conhecimento_like = df[df.iloc[:, 2].astype(str).str.upper().str.contains("CONHECIMENTO", na=False)]
            logger.info(f"📊 Case-insensitive CONHECIMENTO matches: {len(conhecimento_like)}")
            if len(conhecimento_like) > 0:
                conhecimento_df = conhecimento_like

        # Extract codes from first column
        conhecimento_codes = conhecimento_df.iloc[:, 0].astype(str).tolist()

        return conhecimento_codes

    except Exception as e:
        logger.error(f"❌ Error reading classes.xlsx: {e}")
        import traceback
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        return []


def _detect_columns(df, candidates: List[str]) -> Optional[str]:
    cols = [c.lower() for c in df.columns]
    for cand in candidates:
        if cand.lower() in cols:
            return df.columns[cols.index(cand.lower())]
    for c in df.columns:
        lc = c.lower()
        if any(tok in lc for tok in ["id_proc", "idprocess", "processo"]):
            return c
    return None


def create_filtered_eproc_parquets_spark(spark=None):
    """
    Create filtered parquet files for EPROC eligible processes using PySpark.

    This function:
    1. Loads CONHECIMENTO codes from classes.xlsx
    2. Loads EPROC class hierarchy from bronze tables
    3. Filters processes by CONHECIMENTO class
    4. Saves filtered id_processo list to parquet

    Args:
        spark: Optional SparkSession. If not provided, creates one.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import StringType

    logger.info("🚀 Creating filtered EPROC parquet files...")

    # Create or use provided SparkSession
    if spark is None:
        spark = (SparkSession.builder
                 .appName("CreateFilteredEprocParquets")
                 .config("spark.sql.parquet.enableVectorizedReader", "false")
                 .config("spark.sql.parquet.int96AsTimestamp", "true")
                 .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
                 .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
                 .getOrCreate())
        created_spark = True
    else:
        created_spark = False

    try:
        # Step 1: Load CONHECIMENTO codes from Excel
        conhecimento_codes = load_conhecimento_codes_from_excel()
        if not conhecimento_codes:
            logger.error("❌ Failed to load CONHECIMENTO codes from classes.xlsx")
            return None

        logger.info(f"📋 Found {len(conhecimento_codes)} CONHECIMENTO procedure codes")

        # Step 2: Load EPROC class hierarchy tables from bronze layer
        mni_classe_path = str(EPROC_BRONZE_BASE / "mni_classe_judicial")
        relacao_classe_path = str(EPROC_BRONZE_BASE / "relacao_classe_judicial_mni")
        processo_path = str(EPROC_BRONZE_BASE / "processo")

        # Validate paths exist
        for path, name in [(mni_classe_path, "mni_classe_judicial"),
                          (relacao_classe_path, "relacao_classe_judicial_mni"),
                          (processo_path, "processo")]:
            if not os.path.exists(path):
                logger.error(f"❌ Path does not exist: {path}")
                logger.error(f"💡 Please check if EPROC bronze data is available")
                return None

        logger.info(f"📂 Loading mni_classe_judicial from: {mni_classe_path}")
        mni_classe_df = spark.read.parquet(mni_classe_path)

        logger.info(f"📂 Loading relacao_classe_judicial_mni from: {relacao_classe_path}")
        relacao_classe_df = spark.read.parquet(relacao_classe_path)

        logger.info(f"📂 Loading processo from: {processo_path}")
        processo_df = spark.read.parquet(processo_path)

        # Step 3: Analyze intersection between codes
        unique_mni_codes = mni_classe_df.select("cod_mni_classe_judicial").distinct().collect()
        unique_mni_list = [row.cod_mni_classe_judicial for row in unique_mni_codes]

        conhecimento_set = set(conhecimento_codes)
        mni_set = set(unique_mni_list)
        intersection = conhecimento_set.intersection(mni_set)

        logger.info(f"🔍 Intersection analysis:")
        logger.info(f"   📊 CONHECIMENTO codes: {len(conhecimento_codes)}")
        logger.info(f"   📊 MNI codes in DB: {len(unique_mni_list)}")
        logger.info(f"   ✅ Matching codes: {len(intersection)}")

        # Step 4: Filter mni_classe_judicial by CONHECIMENTO codes
        logger.info("🔍 Filtering MNI classes by CONHECIMENTO codes...")
        filtered_mni = mni_classe_df.filter(
            col("cod_mni_classe_judicial").isin(conhecimento_codes)
        )

        # Step 5: Join hierarchy to get eligible id_classe_judicial values
        logger.info("🔗 Joining with relacao_classe_judicial_mni...")
        eligible_classes = filtered_mni.join(
            relacao_classe_df,
            filtered_mni.cod_mni_classe_judicial == relacao_classe_df.cod_mni_classe_judicial,
            "inner"
        ).select(relacao_classe_df.id_classe_judicial).distinct()

        # Step 6: Get eligible processes
        logger.info("🔗 Joining with processo to get eligible id_processo...")
        eligible_processes = processo_df.join(
            eligible_classes,
            "id_classe_judicial",
            "inner"
        ).select("id_processo").distinct()

        # Convert id_processo to StringType for BigInt compatibility
        eligible_processes = eligible_processes.withColumn(
            "id_processo", col("id_processo").cast(StringType())
        )

        count = eligible_processes.count()
        logger.info(f"✅ Found {count:,} eligible processes")

        # Step 7: Save base eligible (classe conhecimento)
        os.makedirs(PARQUET_DIR, exist_ok=True)
        base_output = PARQUET_DIR / "filtered_id_processo_classe_conhecimento.parquet"
        logger.info(f"💾 Saving base eligible to: {base_output}")
        eligible_processes.write.mode("overwrite").parquet(str(base_output))
        logger.info(f"✅ Saved {count:,} id_processo to {base_output}")

        # Step 8: Save meta_01 eligible copy (same set, meta_01 uses eligible_processes mode)
        meta01_out = PARQUET_DIR / "meta_01" / "eligible_id_processo.parquet"
        meta01_out.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"💾 Saving meta_01 eligible to: {meta01_out}")
        eligible_processes.write.mode("overwrite").parquet(str(meta01_out))

        # Step 9: Assunto-based filters (mirror EJUD assunto generator)
        assunto_path = next((p for p in ASSUNTO_CONFIG_PATHS if p.exists()), None)
        if not assunto_path:
            logger.warning(f"⚠️ Assunto config not found (looked for: {ASSUNTO_CONFIG_PATHS}); skipping assunto filters")
            return eligible_processes

        with assunto_path.open("r", encoding="utf-8") as fh:
            assunto_cfg = json.load(fh)
        assunto_groups = assunto_cfg.get("assunto_groups") or assunto_cfg.get("assuntos_grupos") or {}
        if not assunto_groups:
            logger.warning("⚠️ No assunto_groups found in config; skipping assunto filters")
            return eligible_processes

        # Locate processoassunto dataset (try multiple candidates)
        assunto_dirs = [p for p in EPROC_ASSUNTO_CANDIDATES if p.exists()]
        if not assunto_dirs:
            logger.warning(f"⚠️ No processoassunto dataset found under {EPROC_ASSUNTO_CANDIDATES}; skipping assunto filters")
            return eligible_processes

        assunto_paths = []
        for base in assunto_dirs:
            assunto_paths.extend(glob.glob(f"{base}/**/*.parquet", recursive=True))
        if not assunto_paths:
            logger.warning("⚠️ No parquet files found for processoassunto; skipping assunto filters")
            return eligible_processes

        logger.info(f"📂 Loading processoassunto ({len(assunto_paths)} files)...")
        df_assunto = spark.read.parquet(*assunto_paths)

        proc_col = _detect_columns(df_assunto, ["id_processo", "idprocesso"])
        ass_col = _detect_columns(df_assunto, ["codass", "id_assunto", "cod_assunto", "idassunto"])

        if proc_col is None or ass_col is None:
            logger.warning(f"⚠️ Could not detect process/assunto columns (found {df_assunto.columns}); skipping assunto filters")
            return eligible_processes

        logger.info(f"✅ Detected columns: process={proc_col}, assunto={ass_col}")
        eligible_cached = eligible_processes.persist()

        for group_name, codass_values in assunto_groups.items():
            logger.info(f"🎯 Processing assunto group {group_name} ({len(codass_values)} codes)")
            df_group = df_assunto.filter(df_assunto[ass_col].isin(codass_values))
            if df_group is None:
                logger.warning(f"⚠️ No records for {group_name}")
                continue
            df_group = df_group.select(proc_col).distinct()
            df_result = df_group.join(eligible_cached.select("id_processo"), df_group[proc_col] == eligible_cached["id_processo"], "inner")
            df_result = df_result.select(eligible_cached["id_processo"]).distinct()

            out_path = PARQUET_DIR / f"filtered_id_processo_{group_name}.parquet"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"💾 Saving {group_name} to {out_path}")
            df_result.write.mode("overwrite").parquet(str(out_path))

        return eligible_processes

    except Exception as e:
        logger.error(f"❌ Error creating filtered EPROC parquets: {e}")
        import traceback
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        return None

    finally:
        if created_spark:
            spark.stop()


def create_filtered_eproc_parquets_polars():
    """
    Polars fallback – only base CONHECIMENTO filter for now.
    """
    import polars as pl

    logger.info("🚀 Creating filtered EPROC parquet files (Polars)...")

    try:
        conhecimento_codes = load_conhecimento_codes_from_excel()
        if not conhecimento_codes:
            logger.error("❌ Failed to load CONHECIMENTO codes from classes.xlsx")
            return None

        mni_classe_path = EPROC_BRONZE_BASE / "mni_classe_judicial"
        relacao_classe_path = EPROC_BRONZE_BASE / "relacao_classe_judicial_mni"
        processo_path = EPROC_BRONZE_BASE / "processo"

        for path in [mni_classe_path, relacao_classe_path, processo_path]:
            if not path.exists():
                logger.error(f"❌ Path does not exist: {path}")
                return None

        filtered_mni = pl.read_parquet(f"{mni_classe_path}/*.parquet").filter(
            pl.col("cod_mni_classe_judicial").is_in(conhecimento_codes)
        )
        relacao_classe_df = pl.read_parquet(f"{relacao_classe_path}/*.parquet")
        processo_df = pl.read_parquet(f"{processo_path}/*.parquet")

        eligible_classes = filtered_mni.join(
            relacao_classe_df,
            on="cod_mni_classe_judicial",
            how="inner"
        ).select("id_classe_judicial").unique()

        eligible_processes = processo_df.join(
            eligible_classes,
            on="id_classe_judicial",
            how="inner"
        ).select("id_processo").unique()
        eligible_processes = eligible_processes.with_columns(
            pl.col("id_processo").cast(pl.Utf8)
        )

        os.makedirs(PARQUET_DIR, exist_ok=True)
        base_output = PARQUET_DIR / "filtered_id_processo_classe_conhecimento.parquet"
        eligible_processes.write_parquet(str(base_output))
        logger.info(f"✅ Saved base eligible to {base_output}")

        meta01_out = PARQUET_DIR / "meta_01" / "eligible_id_processo.parquet"
        meta01_out.parent.mkdir(parents=True, exist_ok=True)
        eligible_processes.write_parquet(str(meta01_out))
        logger.info(f"✅ Saved meta_01 eligible to {meta01_out}")

        logger.info("⚠️ Assunto filters are available only in Spark mode")
        return eligible_processes

    except Exception as e:
        logger.error(f"❌ Error creating filtered EPROC parquets: {e}")
        import traceback
        logger.error(f"📋 Traceback: {traceback.format_exc()}")
        return None


def main():
    """Main entry point."""
    logger.info("🚀 Starting EPROC eligible process filter generation...")

    try:
        # Try PySpark first
        from pyspark.sql import SparkSession
        logger.info("✅ PySpark available - using Spark for processing")
        create_filtered_eproc_parquets_spark()
    except ImportError:
        # Fall back to Polars
        logger.warning("⚠️ PySpark not available - falling back to Polars")
        create_filtered_eproc_parquets_polars()

    logger.info("\n🎉 EPROC filter generation completed!")


if __name__ == "__main__":
    main()
