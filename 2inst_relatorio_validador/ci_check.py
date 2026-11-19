"""
CI gate script — run in Azure DevOps pipeline before release.
Exits 1 (fails pipeline) if any required column has nulls.

Usage:
    python ci_check.py

Or override config with CLI args:
    python ci_check.py --not-null id_processo dth_referencia --max-rows 100000
"""

import argparse
import sys

import pandas as pd

from utils import load_config, build_s3_path, resolve_aws_profile, get_s3_fs


def main():
    cfg = load_config()

    parser = argparse.ArgumentParser(description="Data quality gate check")
    parser.add_argument("--not-null", nargs="*", default=cfg.get("not_null_cols", []))
    parser.add_argument("--max-rows", type=int, default=cfg.get("max_rows", 100_000))
    args = parser.parse_args()

    aws_profile = resolve_aws_profile(cfg)
    s3_path = build_s3_path(
        cfg.get("s3_bucket", ""),
        cfg.get("s3_prefix", ""),
        cfg.get("s3_projeto", ""),
        cfg.get("s3_tabela", ""),
    )
    print(f"Loading: {s3_path}")

    fs = get_s3_fs(aws_profile)

    files = (
        fs.glob(s3_path.rstrip("/") + "/**/*.parquet")
        or fs.glob(s3_path.rstrip("/") + "/*.parquet")
    )

    dfs, remaining = [], args.max_rows
    for f in sorted(files):
        with fs.open(f, "rb") as fh:
            chunk = pd.read_parquet(fh)
        chunk = chunk.iloc[:remaining]
        dfs.append(chunk)
        remaining -= len(chunk)
        if remaining <= 0:
            break

    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(df):,} rows x {len(df.columns)} columns\n")

    failures = []
    for col in args.not_null:
        if col not in df.columns:
            failures.append(f"  MISSING column: {col}")
            continue
        count = int(df[col].isna().sum())
        pct = count / len(df) * 100 if len(df) else 0
        status = "OK" if count == 0 else "FAIL"
        print(f"  [{status}] {col}: {count} nulls ({pct:.2f}%)")
        if count > 0:
            failures.append(f"  FAIL: {col} has {count} nulls ({pct:.2f}%)")

    if failures:
        print("\n=== QUALITY GATE FAILED ===")
        for f in failures:
            print(f)
        sys.exit(1)
    else:
        print("\n=== QUALITY GATE PASSED ===")
        sys.exit(0)


if __name__ == "__main__":
    main()
