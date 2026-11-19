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
import yaml
from pathlib import Path
import pandas as pd
import s3fs


CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


def build_s3_path(cfg: dict) -> str:
    parts = [
        cfg.get("s3_bucket", ""),
        cfg.get("s3_prefix", ""),
        cfg.get("s3_projeto", ""),
        cfg.get("s3_tabela", ""),
    ]
    parts = [p for p in parts if p]
    return "s3://" + "/".join(parts) + "/"


def main():
    cfg = load_config()

    parser = argparse.ArgumentParser(description="Data quality gate check")
    parser.add_argument("--not-null", nargs="*", default=cfg.get("not_null_cols", []))
    parser.add_argument("--max-rows", type=int, default=cfg.get("max_rows", 100_000))
    parser.add_argument("--aws-profile", default=cfg.get("aws_profile", ""))
    args = parser.parse_args()

    s3_path = build_s3_path(cfg)
    print(f"Loading: {s3_path}")

    kwargs = {"profile": args.aws_profile} if args.aws_profile else {}
    fs = s3fs.S3FileSystem(**kwargs)

    files = fs.glob(s3_path.rstrip("/") + "/**/*.parquet") or \
            fs.glob(s3_path.rstrip("/") + "/*.parquet")

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
    print(f"Loaded {len(df):,} rows × {len(df.columns)} columns\n")

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
