#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all_metas_emr.py — CNJ Meta 2inst full pipeline orchestrator
=================================================================
Python replacement for run_all_metas_emr.sh.

Why subprocess-based (one spark-submit per step) rather than a shared SparkSession:
  - Isolation: an OOM in any step only kills that step, not the whole pipeline.
  - Restart: --from-step lets you resume after fixing a failure without rerunning
    steps that already completed successfully.
  - Memory: each step gets a clean executor JVM sized for that step's workload.
    A shared session must hold the heaviest step's memory for the full runtime.
  - YARN visibility: each step appears as its own YARN application with its own
    Spark UI, making progress and resource consumption much easier to monitor.

Execution order (ORDER IS CRITICAL):
  1  ejud  Meta 01          cnm01 / julgadom01 / susm01 / desm01 / PendJulg01TG
  2  ejud  Meta 02          dism02 stock  (cutoff = year-3)
  3  ejud  Meta 04          improbidade + adm pública  (cutoff = year-4)
  4  ejud  Meta 06          ambiental                  (cutoff = year-1)
  5  ejud  Meta 07          indígena / quilombola       (cutoff = year-1)
  6  ejud  Meta 08          feminicídio + viol. doméstica (cutoff = year-2)
  7  ejud  Meta 10          criança e adolescente       (cutoff = year-2)
  8  ejud  Meta 11          idosos                      (cutoff = year-1)
  9  eproc2g Meta 01        cnm01 / julgadom01 / susm01 / desm01 / PendJulg01TG
                             ↑ MUST run after ejud Meta 01 (DELETE+APPEND, not overwrite)
  10 meta_aux               tb_ft_meta_aux — one row per process, all systems
                             ↑ MUST run after ALL indicator steps
  11 relatorio              tb_ft_meta_por_oj + tb_ft_meta_por_magistrado (%)
                             ↑ MUST run after meta_aux
  12 relatorio_prim         tb_ft_meta_por_prim_oj + tb_ft_meta_por_prim_magistrado (%)
                             ↑ MUST run after meta_aux (requires cod_prim_* from tb_ft_meta_aux)
  13 relatorio_resp         tb_ft_meta_por_oj_resp + tb_ft_meta_por_mag_resp (%)
                             ↑ MUST run after meta_aux; joins bronze movjulg/movdespa at event date
                               for judgment attribution — non-judgment indicators use cod_prim_*

Usage — download & run on EMR master (PuTTY):

  # Step 1 — download the orchestrator (boto3 always available on EMR)
  python3 -c "import boto3; boto3.client('s3').download_file(
      'sgdai-projetos',
      '2instancia/2inst_metacnj/run_all_metas_emr.py',
      'run_all_metas_emr.py')"

  # Step 2 — run (pipe through grep to filter Spark noise)
  python3 run_all_metas_emr.py --year 2026 2>&1 | \
      grep --line-buffered -E '═|STEP|SKIP|Done|FAILED|Start|ERROR|Exception|metacnj|meta_aux|relatorio|relatorio_prim'

  # --skip-dism is the DEFAULT (dism is historical and stable once written).
  # Pass --rebuild-dism only on the very first run of a new year, or when
  # the bronze situacao data changed and the stock must be recomputed.
  python3 run_all_metas_emr.py --year 2026 --rebuild-dism 2>&1 | \
      grep --line-buffered -E '═|STEP|SKIP|Done|FAILED|Start|ERROR|Exception|metacnj|meta_aux|relatorio|relatorio_prim'

  # Override period window (useful for mid-year in-progress runs)
  python3 run_all_metas_emr.py --year 2026 --period-end 2026-06-30 2>&1 | \
      grep --line-buffered -E '═|STEP|SKIP|Done|FAILED|Start|ERROR|Exception|metacnj|meta_aux|relatorio|relatorio_prim'

  # Resume after a failure at step 9  (steps 1-8 already written to Iceberg)
  python3 run_all_metas_emr.py --year 2026 --from-step 9 2>&1 | \
      grep --line-buffered -E '═|STEP|SKIP|Done|FAILED|Start|ERROR|Exception|metacnj|meta_aux|relatorio|relatorio_prim'

  # Dry-run: print every spark-submit command without executing
  python3 run_all_metas_emr.py --year 2026 --dry-run

Notes on the 2025 historical run:
  Just pass --year 2025.  All cutoff dates are computed automatically
  (cutoff = Dec 31 of year-N).  The period window defaults to 2025-01-01 →
  2025-12-31 which is correct for a completed year.
"""

import argparse
import subprocess
import sys
import time
from datetime import date, datetime
from typing import List, Optional

# ── Infrastructure constants ──────────────────────────────────────────────────
ICEBERG_JAR = (
    "/usr/share/aws/iceberg/lib/"
    "iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar"
)
S3_EJUD  = "s3://sgdai-projetos/2instancia/2inst_metacnj/ejud/scripts"
S3_EPROC = "s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/scripts"

SPARK_BASE: List[str] = [
    "spark-submit",
    "--master",      "yarn",
    "--deploy-mode", "client",
    "--jars",        ICEBERG_JAR,
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _year_end(y: int) -> str:
    return f"{y}-12-31"


def _banner(msg: str) -> None:
    print()
    print("═" * 70)
    print(f"  {msg}")
    print("═" * 70)


def _step(
    label: str,
    cmd: List[str],
    step_n: int,
    dry_run: bool,
) -> None:
    """Print a banner, run cmd, abort the pipeline on non-zero exit."""
    print()
    print("═" * 70)
    print(f"  STEP {step_n:>2} │ {label}")
    print(f"  Start: {_now()}")
    print("═" * 70)
    if dry_run:
        print("  [DRY-RUN]", " ".join(cmd))
        print("═" * 70)
        return

    t0 = time.monotonic()
    result = subprocess.run(cmd)
    elapsed = int(time.monotonic() - t0)
    if result.returncode != 0:
        print(f"\n  ✗ FAILED (exit {result.returncode}) after {elapsed}s")
        print(f"  To resume from this step: --from-step {step_n}")
        print("═" * 70)
        sys.exit(result.returncode)
    print(f"  ✓ Done: {_now()}  ({elapsed}s)")
    print("═" * 70)


# ── Pipeline ──────────────────────────────────────────────────────────────────
def run(
    year: int,
    period_start: str,
    period_end: str,
    from_step: int,
    dry_run: bool,
    skip_dism: bool = True,
) -> None:
    # Cutoff dates — CNJ spec: Dec 31 of (year - N)
    cutoffs = {
        "02": _year_end(year - 3),
        "04": _year_end(year - 4),
        "06": _year_end(year - 1),
        "07": _year_end(year - 1),
        "08": _year_end(year - 2),
        "10": _year_end(year - 2),
        "11": _year_end(year - 1),
    }

    _banner(
        f"2inst CNJ Metas — full pipeline\n"
        f"  year={year}  period={period_start} → {period_end}\n"
        f"  from_step={from_step}  skip_dism={skip_dism}  dry_run={dry_run}\n"
        f"  Launch: {_now()}"
    )

    _skip = ["--skip-dism"] if skip_dism else []

    steps: List[tuple] = [
        # (step_n, label, cmd)
        (1,
         "ejud Meta 01  │ cnm01 / julgadom01 / susm01 / desm01",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_m01_emr.py",
             "--year",         str(year),
             "--period-start", period_start,
             "--period-end",   period_end,
         ]),
        (2,
         f"ejud Meta 02  │ dism02 stock  (cutoff={cutoffs['02']})",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_m02_emr.py",
             "--year",        str(year),
             "--cutoff-date", cutoffs["02"],
             "--period-end",  period_end,
         ] + _skip),
        (3,
         f"ejud Meta 04  │ improbidade + adm pública  (cutoff={cutoffs['04']})",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_mNN_emr.py",
             "--meta",        "04",
             "--year",        str(year),
             "--cutoff-date", cutoffs["04"],
             "--period-end",  period_end,
         ] + _skip),
        (4,
         f"ejud Meta 06  │ ambiental  (cutoff={cutoffs['06']})",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_mNN_emr.py",
             "--meta",        "06",
             "--year",        str(year),
             "--cutoff-date", cutoffs["06"],
             "--period-end",  period_end,
         ] + _skip),
        (5,
         f"ejud Meta 07  │ indígena / quilombola  (cutoff={cutoffs['07']})",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_mNN_emr.py",
             "--meta",        "07",
             "--year",        str(year),
             "--cutoff-date", cutoffs["07"],
             "--period-end",  period_end,
         ] + _skip),
        (6,
         f"ejud Meta 08  │ feminicídio + viol. doméstica  (cutoff={cutoffs['08']})",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_mNN_emr.py",
             "--meta",        "08",
             "--year",        str(year),
             "--cutoff-date", cutoffs["08"],
             "--period-end",  period_end,
         ] + _skip),
        (7,
         f"ejud Meta 10  │ criança e adolescente  (cutoff={cutoffs['10']})",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_mNN_emr.py",
             "--meta",        "10",
             "--year",        str(year),
             "--cutoff-date", cutoffs["10"],
             "--period-end",  period_end,
         ] + _skip),
        (8,
         f"ejud Meta 11  │ idosos  (cutoff={cutoffs['11']})",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_mNN_emr.py",
             "--meta",        "11",
             "--year",        str(year),
             "--cutoff-date", cutoffs["11"],
             "--period-end",  period_end,
         ] + _skip),
        (9,
         "eproc2g Meta 01  │ cnm01 / julgadom01 / susm01 / desm01  (cod_sistema=2)",
         SPARK_BASE + [
             "--py-files", f"{S3_EPROC}/eproc_metacnj.zip",
             f"{S3_EPROC}/eproc_metacnj_m01_emr.py",
             "--year",         str(year),
             "--period-start", period_start,
             "--period-end",   period_end,
         ]),
        (10,
         "meta_aux  │ tb_ft_meta_aux — one row per process, all systems",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_meta_aux_emr.py",
             "--year", str(year),
         ]),
        (11,
         "relatorio  │ tb_ft_meta_por_oj + tb_ft_meta_por_magistrado (%)",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_relatorio_emr.py",
             "--year", str(year),
         ]),
        (12,
         "relatorio_prim  │ tb_ft_meta_por_prim_oj + tb_ft_meta_por_prim_magistrado (%)",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_relatorio_prim_emr.py",
             "--year", str(year),
         ]),
        (13,
         "relatorio_resp  │ tb_ft_meta_por_oj_resp + tb_ft_meta_por_mag_resp (%)",
         SPARK_BASE + [
             f"{S3_EJUD}/ejud_metacnj_relatorio_resp_emr.py",
             "--year", str(year),
         ]),
    ]

    t_start = time.monotonic()
    for step_n, label, cmd in steps:
        if step_n < from_step:
            print(f"  [SKIP] step {step_n:>2} — {label}")
            continue
        _step(label, cmd, step_n, dry_run)

    total = int(time.monotonic() - t_start)
    _banner(
        f"ALL STEPS COMPLETE — year={year}\n"
        f"  Finish: {_now()}  (total {total // 60}m {total % 60}s)"
    )


# ── Argparse ──────────────────────────────────────────────────────────────────
def _parse(argv=None):
    today = date.today()
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    p.add_argument(
        "--year", type=int, default=today.year,
        help="Stock year / meta_ano  (default: current calendar year)",
    )
    p.add_argument(
        "--period-start", default=None,
        metavar="YYYY-MM-DD",
        help="First day of the evaluation window  (default: YEAR-01-01)",
    )
    p.add_argument(
        "--period-end", default=None,
        metavar="YYYY-MM-DD",
        help="Last day of the evaluation window  (default: YEAR-12-31)",
    )
    p.add_argument(
        "--from-step", type=int, default=1, metavar="N",
        help="First step to execute; skip steps 1 .. N-1  (default: 1 = full run)",
    )
    p.add_argument(
        "--rebuild-dism", action="store_true",
        help=(
            "Force full recompute of dism stock (steps 2-8). "
            "Use only on the first run of a new year or after bronze data changes. "
            "Default: skip dism (load from already-written Iceberg partitions)."
        ),
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print spark-submit commands without executing them",
    )
    return p.parse_args(argv)


def main(argv=None):
    args = _parse(argv)
    period_start = args.period_start or f"{args.year}-01-01"
    period_end   = args.period_end   or f"{args.year}-12-31"
    run(
        year=args.year,
        period_start=period_start,
        period_end=period_end,
        from_step=args.from_step,
        dry_run=args.dry_run,
        skip_dism=not args.rebuild_dism,
    )


if __name__ == "__main__":
    main()
