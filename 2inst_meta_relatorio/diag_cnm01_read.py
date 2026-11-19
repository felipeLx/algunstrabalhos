#!/usr/bin/env python3
"""
Diagnostic helper: inspect cnm01 parquet loading and month-level counts.
Usage:
  python scripts/diag_cnm01_read.py [--path /full/path/to/cnm01.parquet]

If --path is omitted the script will try to resolve the path from
./config/variables_config.json -> meta_01.paths.ejud_path + '/cnm01.parquet'

This script prints:
 - total rows
 - distinct id_processo (if present)
 - presence of candidate timestamp columns
 - per-month counts per candidate timestamp column (yyyy-MM)
 - top 5 sample rows

Run on the Spark cluster (not a lightweight local Python environment).
"""
import argparse
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format
import os
import glob

CANDIDATE_TS = ["dthrmov", "dth_ult_distribuicao", "dth_autuacao", "dth", "dth_alteracao", "dth_inicio", "dth_fim", "dth_referencia"]


def resolve_default_path():
    try:
        with open("./config/variables_config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)
        ejud_path = cfg.get("meta_01", {}).get("paths", {}).get("ejud_path")
        if isinstance(ejud_path, list):
            ejud_path = ejud_path[0] if ejud_path else None
        if not ejud_path:
            return None
        # Try to resolve relative config paths via utils.parquet_utils.parquet_path if available
        try:
            from utils.parquet_utils import parquet_path
            resolved = parquet_path(ejud_path)
            # ensure result is a directory/file syspath and append filename if needed
            if resolved.endswith("/"):
                resolved = resolved.rstrip("/")
            # If resolved points to ".../current" append cnm01.parquet
            if resolved.endswith("meta_01/current") or resolved.endswith("meta_01/current"):
                return resolved + "/cnm01.parquet"
            # If resolved is a directory, append cnm01.parquet
            return resolved + ("/cnm01.parquet" if not resolved.endswith("cnm01.parquet") else "")
        except Exception:
            # fallback: ensure no trailing slash issues and return relative path for caller to try
            if ejud_path.endswith("/"):
                ejud_path = ejud_path[:-1]
            return ejud_path + "/cnm01.parquet"
    except Exception:
        return None


def print_div():
    print("" + "-" * 80)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="Full path to cnm01.parquet (file or dir)")
    args = parser.parse_args()

    path = args.path or resolve_default_path()
    if not path:
        print("ERROR: Could not resolve cnm01 path. Pass --path or ensure config/variables_config.json has meta_01.paths.ejud_path set.")
        sys.exit(2)

    spark = SparkSession.builder.appName("diag_cnm01_read").getOrCreate()

    print(f"Reading parquet path: {path}")
    # Before letting Spark read, list parquet files visible to the driver process
    try:
        fs_path = path
        if fs_path.startswith("s3a://"):
            print("Note: path is on S3 (s3a://) — driver filesystem listing skipped")
        else:
            # If a file was passed, list its parent directory
            dir_to_list = fs_path if os.path.isdir(fs_path) else os.path.dirname(fs_path)
            if not dir_to_list:
                dir_to_list = "."
            print(f"Driver-visible filesystem listing for: {dir_to_list}")
            parts = sorted(glob.glob(os.path.join(dir_to_list, "**", "*.parquet"), recursive=True))
            if not parts:
                print("  (no .parquet files found by driver in that directory)")
            else:
                print(f"  {len(parts)} parquet files found (driver-visible). Showing up to 10:")
                for p in parts[:10]:
                    try:
                        s = os.path.getsize(p)
                    except Exception:
                        s = None
                    print(f"    {p}  size={s}")
    except Exception as e:
        print(f"Could not list driver filesystem parquet files: {e}")

    try:
        df = spark.read.option("recursiveFileLookup", "true").parquet(path)
    except Exception as e:
        print(f"ERROR: failed to read parquet at {path}: {e}")
        spark.stop()
        sys.exit(3)

    print_div()
    
    # Show which files Spark actually considers part of this DataFrame (cheap)
    try:
        input_files = df.inputFiles()
        print(f"Spark sees {len(input_files)} input files (first 10):")
        for f in input_files[:10]:
            try:
                print(f"  {f}")
            except Exception:
                pass
        try:
            parts_count = df.rdd.getNumPartitions()
            print(f"DataFrame RDD partitions: {parts_count}")
        except Exception:
            pass
    except Exception as e:
        print(f"Could not get DataFrame input files: {e}")

    # If we have driver-visible part-file list, attempt to read a few part files directly to inspect
    try:
        dir_to_list = path if os.path.isdir(path) else os.path.dirname(path)
        parts = sorted(glob.glob(os.path.join(dir_to_list, "**", "*.parquet"), recursive=True))
        if parts:
            print("Attempting to inspect up to 5 individual part files with Spark:")
            for p in parts[:5]:
                try:
                    subdf = spark.read.parquet(p)
                    print(f"  Part file: {p} -> rows: {subdf.count()}, cols: {len(subdf.columns)}")
                    try:
                        subdf.printSchema()
                    except Exception:
                        pass
                except Exception as e:
                    print(f"  Could not read part file {p} with Spark: {e}")
        else:
            print("No driver-visible part files to inspect individually.")
    except Exception as e:
        print(f"Could not inspect individual part files: {e}")

    # check key candidates
    cols = df.columns
    key_candidates = [c for c in cols if c.lower() in ("coddoc", "id_processo")]
    print(f"Columns available ({len(cols)}): {cols}")
    print(f"Candidate key columns: {key_candidates}")

    if key_candidates:
        key = key_candidates[0]
        try:
            distinct_keys = df.select(key).distinct().count()
            print(f"Distinct values in {key}: {distinct_keys}")
        except Exception as e:
            print(f"Could not compute distinct {key} count: {e}")

    # Candidate timestamp columns and per-month counts
    present_ts = [c for c in cols if c.lower() in [x.lower() for x in CANDIDATE_TS]]
    print(f"Candidate timestamp columns present: {present_ts}")

    for ts in present_ts:
        print_div()
        print(f"Per-month counts for column: {ts}")
        try:
            # cast to timestamp and group by yyyy-MM
            month_df = (
                df.filter(col(ts).isNotNull())
                  .withColumn("_ts", to_timestamp(col(ts)))
                  .withColumn("_ym", date_format(col("_ts"), "yyyy-MM"))
                  .groupBy("_ym").count()
                  .orderBy("_ym")
            )
            rows = month_df.collect()
            if not rows:
                print("  (no non-null values after parsing)")
            else:
                for r in rows:
                    print(f"  {r['_ym']}: {r['count']}")
        except Exception as e:
            print(f"  Could not compute per-month counts for {ts}: {e}")

    print_div()
    print("Top 5 sample rows (driver-collected as dicts):")
    try:
        samples = df.limit(5).collect()
        for i, r in enumerate(samples):
            try:
                print(f"Row {i}: {r.asDict()}")
            except Exception:
                print(f"Row {i}: (could not render)")
    except Exception as e:
        print(f"Could not collect sample rows: {e}")

    print_div()
    print("Done")
    spark.stop()


if __name__ == '__main__':
    main()
