from pathlib import Path
import polars as pl
import os
import json
import glob
import logging

# Import helper from utils.parquet_utils. When this file is executed as a script
# (via subprocess) relative imports fail with "no known parent package", so try
# several fallbacks: relative import, package import, then add script dir to
# sys.path and import as a top-level module.
try:
    from .parquet_utils import ejud_bronze_base, ejud_situacao_base
except Exception:
    try:
        # When running as a script from project root, this should work
        from utils.parquet_utils import ejud_bronze_base, ejud_situacao_base
    except Exception:
        import sys
        from pathlib import Path as _P
        _script_dir = str(_P(__file__).resolve().parent)
        if _script_dir not in sys.path:
            sys.path.insert(0, _script_dir)
        # finally try importing as a sibling module file
        from parquet_utils import ejud_bronze_base, ejud_situacao_base

# Setup logger
logger = logging.getLogger("create_filtered_assunto_parquets")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR.parent / "config" / "assunto_groups.json"
PARQUET_DIR = SCRIPT_DIR.parent / "parquet_files"
_ejud_base = Path(ejud_bronze_base())
T_PROCESSO_PATH = f"{(_ejud_base / 't_processo' / '**' / '*.parquet').as_posix()}"
T_TIPOPROCESSO_PATH = f"{(_ejud_base / 'tipoprocesso' / '*.parquet').as_posix()}"
PROCESSOASSUNTO_PATH = f"{(_ejud_base / 'processoassunto' / '*.parquet').as_posix()}"
BASE_CODDOC_PATH = PARQUET_DIR / "filtered_coddoc_classe_conhecimento.parquet"
EJUD_SITUACAO_PATH = f"{(Path(ejud_situacao_base()) / '**' / '*.parquet').as_posix()}"
OUTROS_SITUACAO_VALUES = [9, 81, 137]

def read_parquet_files_with_schema_alignment(parquet_files):
    # Defensive: if caller passed an empty list, return an empty DataFrame
    if not parquet_files:
        logger.warning("read_parquet_files_with_schema_alignment called with empty file list; returning empty DataFrame")
        return pl.DataFrame()

    dfs = []
    all_columns = set()
    dtype_map = {}
    # First pass: collect all columns and dtypes
    for file in parquet_files:
        try:
            df = pl.read_parquet(file)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"⚠️ Skipping unreadable parquet file {file}: {e}")
        for col, dtype in zip(df.columns, df.dtypes):
            all_columns.add(col)
            # Robust: always force datetime columns to ms
            if "Datetime" in str(dtype):
                dtype_map[col] = pl.Datetime('ms')
            elif col not in dtype_map:
                dtype_map[col] = dtype
            else:
                prev = dtype_map[col]
                # If any file has Float64, upcast to Float64
                if prev == pl.Float64 or dtype == pl.Float64:
                    dtype_map[col] = pl.Float64
                elif prev == pl.Utf8 or dtype == pl.Utf8:
                    dtype_map[col] = pl.Utf8
                elif prev == pl.Int64 or dtype == pl.Int64:
                    dtype_map[col] = pl.Int64
                elif prev != dtype:
                    dtype_map[col] = pl.Utf8
    all_columns = list(all_columns)
    datetime_cols = {col for col, dtype in dtype_map.items() if "Datetime" in str(dtype)}
    float_cols = {col for col, dtype in dtype_map.items() if dtype == pl.Float64}
    # Second pass: align schemas
    aligned_dfs = []
    for df in dfs:
        missing = set(all_columns) - set(df.columns)
        for col in missing:
            if col in datetime_cols:
                df = df.with_columns(pl.lit(None, dtype=pl.Datetime('ms')).alias(col))
            elif col in float_cols:
                df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
            else:
                df = df.with_columns(pl.lit(None, dtype=dtype_map[col]).alias(col))
        df = df.select(all_columns)
        # Always cast ALL datetime columns to ms, even if already present
        for col in datetime_cols:
            df = df.with_columns(pl.col(col).cast(pl.Datetime('ms')).alias(col))
        # Always cast ALL float columns to Float64, even if already present
        for col in float_cols:
            df = df.with_columns(pl.col(col).cast(pl.Float64).alias(col))
        aligned_dfs.append(df)
    # If no files could be read (aligned_dfs empty) return empty DataFrame instead of raising
    if not aligned_dfs:
        logger.warning("No readable parquet files after alignment; returning empty DataFrame")
        return pl.DataFrame()

    df_all = pl.concat(aligned_dfs, rechunk=True)
    # Try to cast known integer columns back to Int64 if all values are integer-like
    int_candidate_cols = [c for c in dtype_map if c.lower() in ["coddoc", "codtipproc", "codass", "codextcnj"]]
    for col in int_candidate_cols:
        if col in df_all.columns and df_all[col].dtype == pl.Float64:
            s = df_all[col].drop_nulls()
            if (s % 1 == 0).all():
                df_all = df_all.with_columns(pl.col(col).cast(pl.Int64).alias(col))
    return df_all


def expand_and_log_glob(pattern: str, label: str, max_examples: int = 10):
    """Expand glob pattern, log diagnostic info and return sorted file list."""
    # Enable recursive globbing for ** patterns
    files = sorted(glob.glob(str(pattern), recursive=True))
    logger.info(f"🔎 Glob expand for {label}: pattern={pattern}")
    logger.info(f"   → Found {len(files)} files")
    if files:
        examples = files[:max_examples]
        for ex in examples:
            logger.info(f"      - {ex}")
    else:
        # Provide parent directory diagnostics
        parent = os.path.dirname(pattern.replace('**', '').replace('*', '')) or '/'
        logger.warning(f"   → No files found for pattern. Checking parent directory: {parent}")
        try:
            if os.path.exists(parent):
                entries = os.listdir(parent)
                logger.info(f"      Parent dir exists and contains {len(entries)} entries; sample up to {max_examples}:")
                for e in entries[:max_examples]:
                    logger.info(f"         - {e}")
            else:
                logger.warning(f"      Parent directory does not exist: {parent}")
        except Exception as e:
            logger.warning(f"      Failed to inspect parent directory {parent}: {e}")
    return files

def create_filtered_classe_parquets():
    """
    Create parquet files for each class group defined in the JSON config.
    Each file contains unique coddoc from t_processo where codtipproc matches the class list.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    classes = config.get("classes", {})

    parquet_files = expand_and_log_glob(T_PROCESSO_PATH, 't_processo')
    if not parquet_files:
        logger.error(f"No parquet files found at {T_PROCESSO_PATH}")
        return
    df_processo = read_parquet_files_with_schema_alignment(parquet_files)
    logger.info(f"   📊 Loaded {df_processo.height:,} t_processo records")
    
    # Apply codcompt filter to exclude specific competence codes (1, 9, 12, 21).
    # NULL values must be preserved — Polars ~is_in() returns null for null inputs,
    # so without the is_null() guard those rows would be silently dropped.
    excluded_codcompt = [1, 9, 12, 21]
    if "codcompt" in df_processo.columns:
        initial_count = df_processo.height
        df_processo = df_processo.filter(
            pl.col("codcompt").is_null() | ~pl.col("codcompt").is_in(excluded_codcompt)
        )
        filtered_count = df_processo.height
        logger.info(f"   🚫 Excluded {initial_count - filtered_count:,} records with codcompt in {excluded_codcompt} (nulls preserved)")
        logger.info(f"   ✅ Remaining after codcompt filter: {filtered_count:,} t_processo records")
    else:
        logger.warning(f"   ⚠️ codcompt column not found in t_processo data - skipping competence filter")
        logger.info(f"   📋 Available columns: {', '.join(df_processo.columns)}")

    for class_name, codtipproc_list in classes.items():
        logger.info(f"\n   📂 Processing class: {class_name} ({len(codtipproc_list)} codtipproc)")
        df_filtered = df_processo.filter(pl.col("codtipproc").is_in(codtipproc_list))
        df_coddoc = df_filtered.select("coddoc").unique().sort("coddoc")
        out_path = PARQUET_DIR / f"filtered_coddoc_classe_{class_name}.parquet"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df_coddoc.write_parquet(str(out_path))
        logger.info(f"      ✅ Saved {out_path}: {df_coddoc.height} coddoc")


def create_filtered_assunto_parquets():
    """
    Create parquet files for each assunto group defined in the JSON config.
    Each file contains unique coddoc from filtered_coddoc_classe_conhecimento joined with processoassunto by coddoc/codass.
    """
    config_path = CONFIG_PATH
    parquet_dir = PARQUET_DIR
    processoassunto_path = PROCESSOASSUNTO_PATH
    base_coddoc_path = BASE_CODDOC_PATH

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    # Accept both 'assunto_groups' and 'assuntos_grupos' for robustness
    assunto_groups = config.get("assunto_groups") or config.get("assuntos_grupos") or {}
    logger.info("Loaded config keys: %s", config.keys())
    logger.info("🔄 Starting creation of filtered assunto parquet files...")
    logger.info("📁 processoassunto path: %s", processoassunto_path)
    logger.info("💾 Output base path: %s", parquet_dir)
    logger.info("📊 Creating %d assunto parquet files", len(assunto_groups))

    parquet_files = sorted(glob.glob(str(PROCESSOASSUNTO_PATH), recursive=True))
    if not parquet_files:
        logger.error(f"No parquet files found at {PROCESSOASSUNTO_PATH}")
        return
    df_assunto = read_parquet_files_with_schema_alignment(parquet_files)
    logger.info(f"   📊 Loaded {df_assunto.height:,} processoassunto records")

    df_base_coddoc = pl.read_parquet(base_coddoc_path)
    logger.info(f"   📊 Loaded {df_base_coddoc.height:,} base coddoc records from {base_coddoc_path}")

    # Find codass/coddoc columns dynamically
    codass_col = next((c for c in df_assunto.columns if "codass" in c.lower()), "codass")
    coddoc_col = next((c for c in df_assunto.columns if "coddoc" in c.lower()), "coddoc")

    for group_name, codass_values in assunto_groups.items():
        logger.info(f"\n   📂 Processing {group_name} ({len(codass_values)} codass)")
        df_assunto_filtered = df_assunto.filter(pl.col(codass_col).is_in(codass_values))
        if df_assunto_filtered.height == 0:
            logger.warning(f"      No processo assunto records found for {group_name}, skipping...")
            continue
        df_result = df_base_coddoc.join(
            df_assunto_filtered.select([coddoc_col]).unique(),
            left_on="coddoc", right_on=coddoc_col, how="inner"
        )
        df_final = df_result.select(["coddoc"]).unique().sort("coddoc")
        out_path = f"{parquet_dir}/filtered_coddoc_{group_name}.parquet"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df_final.write_parquet(out_path)
        logger.info(f"      ✅ Saved {out_path}: {df_final.height} coddoc")


def create_all_filtered_parquets():
    """
    Generate all filtered parquet files for both classes and assunto groups.
    For each class in config, generate filtered_coddoc_classe_<class>.parquet.
    For each assunto group, generate filtered_coddoc_<group>.parquet for both conhecimento.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    classes = config.get("classes", {})
    assunto_groups = config.get("assunto_groups") or config.get("assuntos_grupos") or {}

    # --- Classe-based parquets ---
    parquet_files = expand_and_log_glob(T_PROCESSO_PATH, 't_processo')
    if not parquet_files:
        logger.error(f"No parquet files found at {T_PROCESSO_PATH}")
        return
    df_processo = read_parquet_files_with_schema_alignment(parquet_files)
    logger.info(f"   📊 Loaded {df_processo.height:,} t_processo records")
    # Apply codcompt filter to exclude specific competence codes (1, 9, 12, 21).
    # NULL values must be preserved — Polars ~is_in() returns null for null inputs,
    # so without the is_null() guard those rows would be silently dropped.
    excluded_codcompt = [1, 9, 12, 21]
    if "codcompt" in df_processo.columns:
        initial_count = df_processo.height
        df_processo = df_processo.filter(
            pl.col("codcompt").is_null() | ~pl.col("codcompt").is_in(excluded_codcompt)
        )
        filtered_count = df_processo.height
        logger.info(f"   🚫 Excluded {initial_count - filtered_count:,} records with codcompt in {excluded_codcompt} (nulls preserved)")
        logger.info(f"   ✅ Remaining after codcompt filter: {filtered_count:,} t_processo records")
    else:
        logger.warning(f"   ⚠️ codcompt column not found in t_processo data - skipping competence filter")
        logger.info(f"   📋 Available columns: {', '.join(df_processo.columns)}")

    classe_coddoc_paths = {}
    for class_name, codtipproc_list in classes.items():
        logger.info(f"\n   📂 Processing class: {class_name} ({len(codtipproc_list)} codtipproc)")
        df_filtered = df_processo.filter(pl.col("codtipproc").is_in(codtipproc_list)) | df_processo.filter(pl.col("codextcnj").is_in(codtipproc_list))
        df_coddoc = df_filtered.select("coddoc").unique().sort("coddoc")
        out_path = PARQUET_DIR / f"filtered_coddoc_classe_{class_name}.parquet"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df_coddoc.write_parquet(str(out_path))
        logger.info(f"      ✅ Saved {out_path}: {df_coddoc.height} coddoc")
        classe_coddoc_paths[class_name] = out_path

    # --- Assunto-based parquets for each class ---
    parquet_files_assunto = expand_and_log_glob(PROCESSOASSUNTO_PATH, 'processoassunto')
    if not parquet_files_assunto:
        logger.error(f"No parquet files found at {PROCESSOASSUNTO_PATH}")
        return
    df_assunto = read_parquet_files_with_schema_alignment(parquet_files_assunto)
    logger.info(f"   📊 Loaded {df_assunto.height:,} processoassunto records")

    # Find codass/coddoc columns dynamically
    codass_col = next((c for c in df_assunto.columns if "codass" in c.lower()), "codass")
    coddoc_col = next((c for c in df_assunto.columns if "coddoc" in c.lower()), "coddoc")

    for class_name, coddoc_path in classe_coddoc_paths.items():
        df_base_coddoc = pl.read_parquet(coddoc_path)
        logger.info(f"\n   📊 Loaded {df_base_coddoc.height:,} base coddoc records from {coddoc_path}")
        for group_name, codass_values in assunto_groups.items():
            logger.info(f"\n   📂 Processing {group_name} (class: {class_name}, {len(codass_values)} codass)")
            df_assunto_filtered = df_assunto.filter(pl.col(codass_col).is_in(codass_values))
            if df_assunto_filtered.height == 0:
                logger.warning(f"      No processo assunto records found for {group_name}, skipping...")
                continue
            df_result = df_base_coddoc.join(
                df_assunto_filtered.select([coddoc_col]).unique(),
                left_on="coddoc", right_on=coddoc_col, how="inner"
            )
            df_final = df_result.select(["coddoc"]).unique().sort("coddoc")
            out_path = f"{PARQUET_DIR}/filtered_coddoc_{group_name}_classe_{class_name}.parquet"
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            df_final.write_parquet(out_path)
            logger.info(f"      ✅ Saved {out_path}: {df_final.height} coddoc")


def create_filtered_classe_and_assunto_parquets():
    """
    Generate filtered parquet files for each class and each assunto group (not cross-joined).
    - For each class: filtered_coddoc_classe_<class>.parquet
    - For each assunto group: filtered_coddoc_<group>.parquet (using all coddoc from processoassunto with codass in group)
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    classes = config.get("classes", {})
    assunto_groups = config.get("assunto_groups") or config.get("assuntos_grupos") or {}

    # --- Classe-based parquets ---
    parquet_files = sorted(glob.glob(T_PROCESSO_PATH, recursive=True))

    if not parquet_files:
        logger.error(f"No parquet files found at {T_PROCESSO_PATH}")
        return
    
    df_processo = read_parquet_files_with_schema_alignment(parquet_files)
    logger.info(f"   📊 Loaded {df_processo.height:,} t_processo records, {df_processo.select('coddoc').n_unique():,} distinct coddoc")

    # Apply codcompt filter to exclude specific competence codes (1, 9, 12, 21).
    # NULL values must be preserved — Polars ~is_in() returns null for null inputs,
    # so without the is_null() guard those rows would be silently dropped.
    excluded_codcompt = [1, 9, 12, 21]
    if "codcompt" in df_processo.columns:
        initial_count = df_processo.height
        df_processo = df_processo.filter(
            pl.col("codcompt").is_null() | ~pl.col("codcompt").is_in(excluded_codcompt)
        )
        filtered_count = df_processo.height
        logger.info(f"   🚫 Excluded {initial_count - filtered_count:,} records with codcompt in {excluded_codcompt} (nulls preserved)")
        logger.info(f"   [STEP 0] t_processo after codcompt filter: {filtered_count:,} rows, {df_processo.select('coddoc').n_unique():,} distinct coddoc")
    else:
        logger.warning(f"   ⚠️ codcompt column not found in t_processo data - skipping competence filter")
        logger.info(f"   📋 Available columns: {', '.join(df_processo.columns)}")

    # Check if we have the required columns in t_processo
    has_codextcnj = "codextcnj" in df_processo.columns
    has_codtipproc = "codtipproc" in df_processo.columns

    if not has_codextcnj and not has_codtipproc:
        logger.error("❌ Neither 'codextcnj' nor 'codtipproc' found in t_processo data")
        logger.info(f"   Available columns: {', '.join(df_processo.columns)}")
        return

    logger.info(f"   ✅ Found columns - codextcnj: {has_codextcnj}, codtipproc: {has_codtipproc}")

    # Join with tipoprocesso on codtipproc to obtain codextcnj for class filtering.
    # codnat is NOT used as a filter — codextcnj alone defines eligibility.
    # Some valid conhecimento classes have codnat != 1 (e.g. codextcnj=417 → codnat=2).
    # Both "conhecimento" and "outros" use the same unrestricted join.
    df_joined = df_processo
    df_joined_all = df_processo  # fallback used for 'outros' (no codnat restriction)
    if has_codtipproc:
        tipoprocesso_files = sorted(glob.glob(T_TIPOPROCESSO_PATH, recursive=True))
        if tipoprocesso_files:
            logger.info("   🔄 Loading tipoprocesso for codnat + codextcnj mapping...")
            df_tipoprocesso = read_parquet_files_with_schema_alignment(tipoprocesso_files)
            if df_tipoprocesso.height > 0:
                tp_keep = [c for c in ["codtipproc", "codnat", "codextcnj"] if c in df_tipoprocesso.columns]
                df_tipoprocesso = df_tipoprocesso.select(tp_keep)
                logger.info(f"   📊 Loaded {df_tipoprocesso.height:,} tipoprocesso records (cols: {tp_keep})")
                try:
                    df_processo_cast = df_processo.with_columns(pl.col("codtipproc").cast(pl.String))
                    # Drop codextcnj from t_processo before joining so that after the join,
                    # codextcnj unambiguously comes from tipoprocesso (the authoritative mapping).
                    # If we don't drop it, Polars would keep t_processo.codextcnj as "codextcnj"
                    # and rename tipoprocesso.codextcnj to "codextcnj_right", causing the filter
                    # to use the wrong column (t_processo values that may not be in the config).
                    if "codextcnj" in df_processo_cast.columns:
                        df_processo_cast = df_processo_cast.drop("codextcnj")
                    df_tp_cast = df_tipoprocesso.with_columns(pl.col("codtipproc").cast(pl.String))

                    # --- join: codnat is NOT filtered — codextcnj alone defines eligibility ---
                    # Some valid conhecimento classes have codnat != 1 (e.g. codextcnj=417 → codnat=2).
                    # Both conhecimento and outros use the same unrestricted tipoprocesso join.
                    df_tp_dedup = df_tp_cast.unique(subset=["codtipproc"], keep="first")
                    df_joined = df_processo_cast.join(df_tp_dedup, on="codtipproc", how="inner")
                    logger.info(f"   [STEP 1] tipoprocesso (codnat NOT filtered) → unique(codtipproc): {df_tp_dedup.height:,} distinct codtipproc")
                    logger.info(f"   [STEP 2] t_processo × tipoprocesso inner join: {df_joined.height:,} rows")
                    logger.info(f"   [STEP 2] distinct coddoc after join (before class filter): {df_joined.select('coddoc').n_unique():,}")

                    # --- outros join: same unrestricted join ---
                    df_joined_all = df_joined
                    logger.info(f"   [STEP 2b] outros join (same as conhecimento, no codnat restriction): {df_joined_all.height:,} rows, {df_joined_all.select('coddoc').n_unique():,} distinct coddoc")

                except Exception as e:
                    logger.warning(f"   ⚠️ Failed to join with tipoprocesso: {e}")
            else:
                logger.warning("   ⚠️ Tipoprocesso data is empty")
        else:
            logger.warning(f"   ⚠️ No tipoprocesso files found at {T_TIPOPROCESSO_PATH}")
    
    logger.info(f"   📊 Final joined dataset (conhecimento/cível): {df_joined.height:,} records")
    logger.info(f"   📊 Final joined dataset (outros/all): {df_joined_all.height:,} records")
    
    # Show available columns for debugging
    logger.info(f"   📋 Available columns: {', '.join(df_joined.columns)}")

    # Determine which column to use for filtering (prefer codextcnj — CNJ class code)
    filter_column = "codextcnj" if "codextcnj" in df_joined.columns else "codtipproc"
    logger.info(f"   🎯 Using '{filter_column}' for class filtering")

    # Cast filter column to integer first, then to string to avoid decimal issues
    for _df_name, _df_ref in [("df_joined", df_joined), ("df_joined_all", df_joined_all)]:
        if filter_column in _df_ref.columns:
            try:
                _df_ref = _df_ref.with_columns(
                    pl.col(filter_column).cast(pl.Float64).cast(pl.Int64).cast(pl.String).alias(filter_column)
                )
            except Exception as e:
                logger.warning(f"   ⚠️ Could not convert {filter_column} to integer in {_df_name}, using string cast: {e}")
                _df_ref = _df_ref.with_columns(pl.col(filter_column).cast(pl.String))
            if _df_name == "df_joined":
                df_joined = _df_ref
            else:
                df_joined_all = _df_ref
    logger.info(f"   🔄 Converted '{filter_column}' to string in both dataframes for consistent filtering")

    class_coddocs = {}
    for class_name, filter_values in classes.items():
        logger.info(f"\n   📂 Processing class: {class_name} ({len(filter_values)} {filter_column})")
        filter_values_str = [str(int(float(x))) if str(x).replace('.', '').replace('-', '').isdigit() else str(x) for x in filter_values]

        # Both "conhecimento" and "outros" use the same join (no codnat restriction).
        # codextcnj alone defines eligibility — df_joined and df_joined_all are identical.
        source_df = df_joined if class_name == "conhecimento" else df_joined_all
        n_source = source_df.height
        n_source_coddoc = source_df.select("coddoc").n_unique()
        logger.info(f"      [STEP 3a] source_df for '{class_name}': {n_source:,} rows, {n_source_coddoc:,} distinct coddoc")

        if class_name == "conhecimento":
            logger.info(f"      🔬 codnat NOT filtered — codextcnj alone defines eligibility (e.g. codextcnj=417 has codnat=2)")

        df_filtered = source_df.filter(pl.col(filter_column).is_in(filter_values_str))
        n_filtered_rows = df_filtered.height
        n_filtered_coddoc = df_filtered.select("coddoc").n_unique()
        logger.info(f"      [STEP 3b] after {filter_column} class filter ({len(filter_values_str)} values): {n_filtered_rows:,} rows, {n_filtered_coddoc:,} distinct coddoc")

        df_coddoc = df_filtered.select("coddoc").unique().sort("coddoc")
        logger.info(f"      [STEP 3c] unique coddoc (final eligible for '{class_name}'): {df_coddoc.height:,}")

        # For 'outros', additionally intersect with coddocs present in ejud_situacao
        # where situacao IN (9, 81, 137)
        if class_name == "outros":
            situacao_files = expand_and_log_glob(EJUD_SITUACAO_PATH, "ejud_situacao")
            if situacao_files:
                # Peek at schema from the first file only to discover column names
                # without loading all files into memory.
                peek = pl.read_parquet(situacao_files[0], n_rows=0)
                situacao_col = next((c for c in peek.columns if "situacao" in c.lower()), "situacao")
                coddoc_sit_col = next((c for c in peek.columns if "coddoc" in c.lower()), "coddoc")
                # Use PyArrow dataset API instead of pl.scan_parquet: it handles
                # mixed coddoc types (INT64 in some partitions, STRING in others)
                # by coercing to string via the explicit schema, and still applies
                # predicate pushdown so only matching rows are loaded.
                import pyarrow.dataset as _ds
                import pyarrow as _pa
                _arrow_schema = _pa.schema([
                    _pa.field(coddoc_sit_col, _pa.string()),
                    _pa.field(situacao_col, _pa.string()),
                ])
                _sit_values_str = (
                    [str(v) for v in OUTROS_SITUACAO_VALUES]
                    + [str(v).zfill(3) for v in OUTROS_SITUACAO_VALUES]
                )
                _dataset = _ds.dataset(situacao_files, format="parquet", schema=_arrow_schema)
                _table = _dataset.to_table(
                    columns=[coddoc_sit_col],
                    filter=_ds.field(situacao_col).isin(_sit_values_str),
                )
                df_coddoc_sit = (
                    pl.from_arrow(_table)
                    .rename({coddoc_sit_col: "coddoc"})
                    .with_columns(
                        pl.col("coddoc").cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias("coddoc")
                    )
                    .filter(pl.col("coddoc").is_not_null())
                    .unique()
                )
                logger.info(f"      [STEP 3d] ejud_situacao coddoc with situacao IN {OUTROS_SITUACAO_VALUES}: {df_coddoc_sit.height:,}")
                logger.info(f"      [STEP 3d] outros coddoc before situacao intersect: {df_coddoc.height:,}")
                # Ensure df_coddoc key is also Int64 before joining
                df_coddoc = df_coddoc.with_columns(pl.col("coddoc").cast(pl.Int64, strict=False))
                df_coddoc = df_coddoc.join(df_coddoc_sit, on="coddoc", how="inner").unique().sort("coddoc")
                logger.info(f"      [STEP 3e] outros after situacao intersect (FINAL): {df_coddoc.height:,} coddoc")
            else:
                logger.warning(f"      ⚠️ No ejud_situacao files found at {EJUD_SITUACAO_PATH} — skipping situacao filter for outros")

        class_coddocs[class_name] = df_coddoc

    # Merge 'outros' (with situacao filter already applied) into 'conhecimento' so that
    # downstream metas keep reading a single filtered_coddoc_classe_conhecimento.parquet
    if "outros" in class_coddocs and "conhecimento" in class_coddocs:
        df_outros = class_coddocs.pop("outros")
        n_conhecimento_pre = class_coddocs["conhecimento"].height
        df_merged = pl.concat([class_coddocs["conhecimento"], df_outros]).unique().sort("coddoc")
        logger.info(f"\n   [STEP 4] Merge: conhecimento={n_conhecimento_pre:,} + outros={df_outros.height:,} → {df_merged.height:,} unique coddoc (filtered_coddoc_classe_conhecimento.parquet)")
        class_coddocs["conhecimento"] = df_merged

    for class_name, df_coddoc in class_coddocs.items():
        out_path = PARQUET_DIR / f"filtered_coddoc_classe_{class_name}.parquet"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df_coddoc.write_parquet(str(out_path))
        logger.info(f"       ✅ Saved {out_path}: {df_coddoc.height} coddoc")

    # --- Assunto-based parquets ---
    parquet_files_assunto = sorted(glob.glob(str(PROCESSOASSUNTO_PATH), recursive=True))
    if not parquet_files_assunto:
        logger.error(f"No parquet files found at {PROCESSOASSUNTO_PATH}")
        return
    df_assunto = read_parquet_files_with_schema_alignment(parquet_files_assunto)
    logger.info(f"   📊 Loaded {df_assunto.height:,} processoassunto records")

    # Find codass/coddoc columns dynamically
    codass_col = next((c for c in df_assunto.columns if "codass" in c.lower()), "codass")
    coddoc_col = next((c for c in df_assunto.columns if "coddoc" in c.lower()), "coddoc")

    # Use the in-memory conhecimento universe built above to pre-intersect each assunto group
    # so the output parquets are already scoped to eligible processes.
    df_conhecimento_base = class_coddocs.get("conhecimento")
    if df_conhecimento_base is not None:
        # Ensure coddoc is Int64 for consistent joins
        df_conhecimento_base = df_conhecimento_base.with_columns(
            pl.col("coddoc").cast(pl.Int64, strict=False)
        ).filter(pl.col("coddoc").is_not_null())
        logger.info(f"   📊 Using {df_conhecimento_base.height:,} conhecimento coddocs as base for assunto intersection")
    else:
        logger.warning("   ⚠️ No conhecimento base available — saving assunto parquets without class pre-filter")

    for group_name, codass_values in assunto_groups.items():
        logger.info(f"\n   📂 Processing {group_name} ({len(codass_values)} codass)")
        df_assunto_filtered = df_assunto.filter(pl.col(codass_col).is_in(codass_values))
        if df_assunto_filtered.height == 0:
            logger.warning(f"      No processo assunto records found for {group_name}, skipping...")
            continue
        # Normalise coddoc to Int64 (strips float artefacts like "12345.0")
        df_assunto_coddoc = (
            df_assunto_filtered.select([coddoc_col])
            .with_columns(pl.col(coddoc_col).cast(pl.Float64, strict=False).cast(pl.Int64, strict=False))
            .filter(pl.col(coddoc_col).is_not_null())
            .rename({coddoc_col: "coddoc"})
            .unique()
        )
        if df_conhecimento_base is not None:
            df_final = df_conhecimento_base.join(df_assunto_coddoc, on="coddoc", how="inner").unique().sort("coddoc")
        else:
            df_final = df_assunto_coddoc.sort("coddoc")
        out_path = f"{PARQUET_DIR}/filtered_coddoc_{group_name}.parquet"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df_final.write_parquet(out_path)
        logger.info(f"      ✅ Saved {out_path}: {df_final.height} coddoc (pre-intersected with conhecimento)")


if __name__ == "__main__":
    logger.info("🚀 Creating filtered classe and assunto parquet files for meta queries...")
    create_filtered_classe_and_assunto_parquets()
    logger.info("\n🎉 All parquet files created successfully!")
