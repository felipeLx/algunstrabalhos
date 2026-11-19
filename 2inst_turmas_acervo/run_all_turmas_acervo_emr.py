#!/usr/bin/env python3
"""
Wrapper to launch ejud_turmas_acervo_emr.py via spark-submit on EMR.

Usage:
  python3 run_all_turmas_acervo_emr.py --dateref 2026-04-24
  python3 run_all_turmas_acervo_emr.py --dateref 2026-04-24 --dry-run
"""

import argparse
import subprocess
import sys
from datetime import datetime

S3_BASE    = "s3://sgdai-projetos/2instancia/2inst_turmas_acervo"
S3_SCRIPTS = f"{S3_BASE}/ejud/scripts"
ICEBERG_JAR = "/usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar"


def run(cmd: list, dry_run: bool = False) -> None:
    label = " ".join(cmd[:5]) + " ..."
    print(f"\n[{datetime.now():%H:%M:%S}] STEP: {label}")
    if dry_run:
        print("  DRY-RUN:", " ".join(cmd))
        return
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"ERROR: step failed (rc={result.returncode})")
        sys.exit(result.returncode)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dateref", required=True,
                    help="Reference date YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print commands without executing")
    ap.add_argument("--executor-memory",          default="18g")
    ap.add_argument("--executor-memory-overhead", default="4g")
    ap.add_argument("--shuffle-partitions",       default="64")
    args = ap.parse_args()

    cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--jars", ICEBERG_JAR,
        "--conf", f"spark.executor.memory={args.executor_memory}",
        "--conf", f"spark.executor.memoryOverhead={args.executor_memory_overhead}",
        "--conf", f"spark.sql.shuffle.partitions={args.shuffle_partitions}",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.autoBroadcastJoinThreshold=-1",
        f"{S3_SCRIPTS}/ejud_turmas_acervo_emr.py",
        "--dateref", args.dateref,
        "--executor-memory",          args.executor_memory,
        "--executor-memory-overhead", args.executor_memory_overhead,
        "--shuffle-partitions",       args.shuffle_partitions,
    ]

    start = datetime.now()
    run(cmd, dry_run=args.dry_run)
    elapsed = datetime.now() - start
    print(f"\nDone in {elapsed}. dateref={args.dateref}")


if __name__ == "__main__":
    main()
