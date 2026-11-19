#!/usr/bin/env python3
"""
Config-driven meta runner skeleton.
- Reads two config files: one for paths/variables/stocks, another for date rules.
- Selects the target meta by id (e.g., meta_02_ejud, meta_02_eproc).
- Prepares a merged runtime config and optionally executes the configured script.
This is the first step toward unifying EJUD and EPROC metas under a single runner.
"""
import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

CONFIG_DIR = Path(__file__).parent / "config"
PATHS_CONFIG_FILE = CONFIG_DIR / "meta_runner_paths.json"
DATES_CONFIG_FILE = CONFIG_DIR / "meta_runner_dates.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("meta_runner")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _merge_configs(meta_id: str) -> Dict[str, Any]:
    paths_cfg = _load_json(PATHS_CONFIG_FILE)
    dates_cfg = _load_json(DATES_CONFIG_FILE)

    meta_cfg = paths_cfg.get("metas", {}).get(meta_id)
    if meta_cfg is None:
        raise KeyError(f"Meta id '{meta_id}' not found in {PATHS_CONFIG_FILE}")

    meta_key = meta_cfg.get("meta_key") or meta_cfg.get("meta") or meta_cfg.get("meta_number")
    if meta_key is None:
        raise KeyError(f"Meta id '{meta_id}' is missing 'meta_key' to resolve date rules")

    date_cfg = dates_cfg.get("metas", {}).get(meta_key, {})

    merged: Dict[str, Any] = {}
    merged.update(paths_cfg.get("common", {}))
    merged.update(meta_cfg)
    merged["date_rules"] = date_cfg
    return merged


def _run_subprocess(script_path: str, env: Dict[str, str]) -> int:
    cmd = [sys.executable, script_path]
    logger.info("Running subprocess: %s", " ".join(cmd))
    result = subprocess.run(cmd, env=env, cwd=Path(__file__).parent)
    return result.returncode


def run_meta(meta_id: str, dry_run: bool = True) -> None:
    cfg = _merge_configs(meta_id)

    logger.info("Selected meta: %s | system=%s | meta_key=%s", meta_id, cfg.get("system"), cfg.get("meta_key"))
    logger.info("Stocks: %s", cfg.get("stocks", []))
    logger.info("Variables: %s", cfg.get("variables", []))
    logger.info("Paths: %s", cfg.get("paths", {}))
    logger.info("Date rules: %s", cfg.get("date_rules", {}))

    if dry_run:
        logger.info("Dry run enabled; not executing script")
        return

    script_path = cfg.get("script")
    if not script_path:
        raise KeyError(f"Meta id '{meta_id}' is missing 'script' path in config")

    resolved_script = str((Path(__file__).parent / script_path).resolve())
    if not os.path.exists(resolved_script):
        raise FileNotFoundError(f"Script not found: {resolved_script}")

    env = os.environ.copy()
    env.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g --conf spark.sql.autoBroadcastJoinThreshold=-1 pyspark-shell")

    exit_code = _run_subprocess(resolved_script, env)
    if exit_code != 0:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified meta runner")
    parser.add_argument("--meta", required=True, help="Meta id defined in meta_runner_paths.json (e.g., meta_02_ejud)")
    parser.add_argument("--execute", action="store_true", help="Execute the configured script (otherwise dry-run)")
    args = parser.parse_args()

    run_meta(meta_id=args.meta, dry_run=not args.execute)
