#!/usr/bin/env python3
from pathlib import Path
import glob
import polars as pl
import sys

# Try to import ejud_bronze_base helper from utils.parquet_utils
try:
    from utils.parquet_utils import ejud_bronze_base
except Exception:
    # If running from repository root, allow import from nearby file
    sys.path.insert(0, str(Path(__file__).resolve().parent / "utils"))
    from parquet_utils import ejud_bronze_base

WS = Path(__file__).resolve().parent
PARQUET_DIR = WS / "parquet_files"

# Target assunto code
TARGET_CODASS = 9909

# Filtered parquet filenames to check
IDOSOS_FILE = PARQUET_DIR / "filtered_coddoc_idosos.parquet"
CRIANCA_FILE = PARQUET_DIR / "filtered_coddoc_crianca_adolescente.parquet"

def load_parquet_safe(path: Path):
    if not path.exists():
        print(f"MISSING: {path}")
        return pl.DataFrame()
    try:
        return pl.read_parquet(path)
    except Exception as e:
        print(f"Failed to read {path}: {e}")
        return pl.DataFrame()

def collect_processoassunto_rows():
    bronze_base = Path(ejud_bronze_base())
    candidates = []
    # bronze ejud processoassunto
    candidates.extend(glob.glob(str((bronze_base / "processoassunto" / "*.parquet"))))
    # also check project parquet_files folder for processoassunto files
    candidates.extend(glob.glob(str((PARQUET_DIR / "processoassunto" / "*.parquet"))))
    candidates.extend(glob.glob(str((PARQUET_DIR / "*processoassunto*.parquet"))))
    candidates = sorted(set(candidates))
    if not candidates:
        print("No processoassunto parquet files found in bronze base or project parquet_files.")
        return pl.DataFrame()
    dfs = []
    for p in candidates:
        try:
            dfs.append(pl.read_parquet(p))
        except Exception as e:
            print(f"Warning: failed to read {p}: {e}")
    if not dfs:
        print("No processoassunto data could be read.")
        return pl.DataFrame()
    return pl.concat(dfs, rechunk=True)

def detect_cols(df):
    codass_col = next((c for c in df.columns if "codass" in c.lower()), None)
    coddoc_col = next((c for c in df.columns if "coddoc" in c.lower()), None)
    return codass_col, coddoc_col

def to_int_set(series):
    try:
        return set(int(x) for x in series.drop_nulls().to_list())
    except Exception:
        return set()

def main():
    print("Loading filtered outputs...")
    df_idosos = load_parquet_safe(IDOSOS_FILE)
    df_crianca = load_parquet_safe(CRIANCA_FILE)
    print(f"idosos rows: {df_idosos.height}, crianca rows: {df_crianca.height}")

    print("Collecting processoassunto data...")
    df_assunto = collect_processoassunto_rows()
    if df_assunto.is_empty():
        print("No processoassunto data available; aborting.")
        return

    print(f"Loaded processoassunto rows: {df_assunto.height}")
    codass_col, coddoc_col = detect_cols(df_assunto)
    if not codass_col or not coddoc_col:
        print("Could not detect codass/coddoc columns in processoassunto data; columns:", df_assunto.columns)
        return
    print(f"Detected columns -> codass: {codass_col}, coddoc: {coddoc_col}")

    # Normalize codass to Int64 if possible
    try:
        df_assunto = df_assunto.with_columns(pl.col(codass_col).cast(pl.Int64))
    except Exception:
        pass

    df_target = df_assunto.filter(pl.col(codass_col) == TARGET_CODASS)
    print(f"Rows with {codass_col} == {TARGET_CODASS}: {df_target.height}")
    if df_target.height == 0:
        print("No matching processoassunto rows for target codass.")
    coddocs = df_target.select(pl.col(coddoc_col)).unique().to_series().drop_nulls().to_list()
    coddocs_set = set()
    for v in coddocs:
        try:
            coddocs_set.add(int(v))
        except Exception:
            pass
    print(f"Unique coddocs for codass {TARGET_CODASS}: {len(coddocs_set)}")

    def df_to_set(df):
        if df.is_empty():
            return set()
        col = next((c for c in df.columns if "coddoc" in c.lower()), df.columns[0])
        try:
            vals = df.select(pl.col(col)).unique().to_series().drop_nulls().to_list()
            return set(int(v) for v in vals)
        except Exception:
            return set()

    set_idosos = df_to_set(df_idosos)
    set_crianca = df_to_set(df_crianca)

    inter_idosos = set_idosos & coddocs_set
    inter_crianca = set_crianca & coddocs_set

    print(f"Intersection counts -> idosos: {len(inter_idosos)}, crianca_adolescente: {len(inter_crianca)}")
    print("Sample idosos intersection (up to 50):", sorted(list(inter_idosos))[:50])
    print("Sample crianca intersection (up to 50):", sorted(list(inter_crianca))[:50])

if __name__ == "__main__":
    main()