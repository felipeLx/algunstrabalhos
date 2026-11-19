"""
Row count reconciliation: S3 Bronze vs Oracle source tables.

Reads each table from config.yaml, counts rows in both S3 and Oracle,
flags discrepancies.

Requirements:
  pip install oracledb pandas pyarrow s3fs pyyaml

Oracle password read from env var ORACLE_PASSWORD (or configured in oracle.password_env).
Can also override with --password-env flag.

Usage:
  python compare_bronze_oracle.py
  python compare_bronze_oracle.py --threshold 0.01   # flag diff > 1%
"""

import argparse
import os
import sys
import yaml
from pathlib import Path
from typing import Optional

import pandas as pd
import s3fs
import oracledb


CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


def count_s3_parquet(fs: s3fs.S3FileSystem, bucket: str, rel_path: str) -> Optional[int]:
    full_path = f"{bucket}/{rel_path.strip('/')}"
    files = fs.glob(full_path + "/**/*.parquet") or fs.glob(full_path + "/*.parquet")
    if not files:
        return None
    total = 0
    for f in files:
        meta = fs.info(f)
        if meta.get("size", 0) == 0:
            continue
        with fs.open(f, "rb") as fh:
            pf = pd.read_parquet(fh, columns=[])  # read no columns, only count rows
            total += len(pf)
    return total


def count_oracle(conn: oracledb.Connection, table: str) -> Optional[int]:
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row = cursor.fetchone()
        return int(row[0]) if row else None
    except oracledb.DatabaseError as e:
        print(f"  Oracle error on {table}: {e}")
        return None


def main():
    cfg = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=0.0,
                        help="Flag rows where abs(diff_pct) > threshold (default 0 = any diff)")
    parser.add_argument("--password-env", default=cfg.get("oracle", {}).get("password_env", "ORACLE_PASSWORD"))
    args = parser.parse_args()

    oracle_cfg = cfg.get("oracle", {})
    tables = cfg.get("bronze_tables", [])
    bucket = cfg.get("s3_bucket", "")
    aws_profile = cfg.get("aws_profile", "")

    if not tables:
        print("No bronze_tables configured in config.yaml.")
        sys.exit(1)

    # ─── Oracle connection ────────────────────────────────────────────────────
    password = os.environ.get(args.password_env, "")
    if not password:
        print(f"ERROR: env var {args.password_env} not set.")
        sys.exit(1)

    print(f"Connecting to Oracle: {oracle_cfg['user']}@{oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service_name']}")
    try:
        oracle_conn = oracledb.connect(
            user=oracle_cfg["user"],
            password=password,
            host=oracle_cfg["host"],
            port=oracle_cfg["port"],
            service_name=oracle_cfg["service_name"],
        )
    except oracledb.DatabaseError as e:
        print(f"Oracle connection failed: {e}")
        sys.exit(1)

    # ─── S3 session ───────────────────────────────────────────────────────────
    s3_kwargs = {"profile": aws_profile} if aws_profile else {}
    fs = s3fs.S3FileSystem(**s3_kwargs)

    # ─── Reconcile ───────────────────────────────────────────────────────────
    results = []
    for entry in tables:
        rel_path = entry["s3_path"]
        oracle_table = entry["oracle_table"]

        print(f"\n[{oracle_table}]")

        s3_count = count_s3_parquet(fs, bucket, rel_path)
        ora_count = count_oracle(oracle_conn, oracle_table)

        if s3_count is None:
            status = "NO_S3_FILES"
            diff = None
            diff_pct = None
        elif ora_count is None:
            status = "ORACLE_ERROR"
            diff = None
            diff_pct = None
        else:
            diff = s3_count - ora_count
            diff_pct = round(abs(diff) / ora_count * 100, 4) if ora_count else None
            if diff == 0:
                status = "OK"
            elif diff_pct is not None and diff_pct > args.threshold * 100:
                status = "FAIL"
            else:
                status = "WARN"

        print(f"  S3    : {s3_count:,}" if s3_count is not None else "  S3    : ERROR")
        print(f"  Oracle: {ora_count:,}" if ora_count is not None else "  Oracle: ERROR")
        print(f"  Diff  : {diff:+,} ({diff_pct:.4f}%)" if diff is not None else "  Diff  : N/A")
        print(f"  Status: {status}")

        results.append({
            "table": oracle_table,
            "s3_path": f"s3://{bucket}/{rel_path}",
            "s3_count": s3_count,
            "oracle_count": ora_count,
            "diff": diff,
            "diff_pct": diff_pct,
            "status": status,
        })

    oracle_conn.close()

    # ─── Summary ─────────────────────────────────────────────────────────────
    df = pd.DataFrame(results)
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(df.to_string(index=False))

    fails = df[df["status"] == "FAIL"]
    if not fails.empty:
        print(f"\n{len(fails)} table(s) FAILED reconciliation.")
        sys.exit(1)
    else:
        print("\nAll tables reconciled OK.")
        sys.exit(0)


if __name__ == "__main__":
    main()
