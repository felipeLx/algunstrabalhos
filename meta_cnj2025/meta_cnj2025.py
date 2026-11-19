#!/usr/bin/env python3
import logging
import subprocess
import sys
import os
import time
import gc
import importlib
from pathlib import Path

# Import shared resource factories (used for in-process runs)
try:
    from utils.meta_config_manager import MetaConfigManager
    from utils.meta_logging_system import MetaLogger
    from utils.spark_meta_utils import SparkMetaUtils
    from utils.hybrid_parquet_loader import HybridParquetLoader
    from utils.misc import MetaMiscellaneous
    from utils.meta_results_exporter import MetaResultsExporter
    INPROCESS_CAPABLE = True
except Exception:
    INPROCESS_CAPABLE = False

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from utils.meta_config_manager import MetaConfigManager  # type: ignore
    from utils.meta_logging_system import MetaLogger  # type: ignore
    from utils.spark_meta_utils import SparkMetaUtils  # type: ignore
    from utils.hybrid_parquet_loader import HybridParquetLoader  # type: ignore
    from utils.misc import MetaMiscellaneous  # type: ignore
    from utils.meta_results_exporter import MetaResultsExporter  # type: ignore

from utils.save_to_postgres import save_parquet_to_postgres_table
from utils.misc import safe_stop_spark

META_SCRIPTS = [
    ("meta_01_ejud.py", "meta_01"),
    ("meta_01_eproc.py", "meta_01"),
    ("meta_02_ejud.py", "meta_02"),
    ("meta_04_ejud.py", "meta_04"),
    ("meta_06_ejud.py", "meta_06"),
    ("meta_07_ejud.py", "meta_07"),
    ("meta_08_ejud.py", "meta_08"),
    ("meta_10_ejud.py", "meta_10"),
    ("meta_11_ejud.py", "meta_11"),
]

# Parquet generator runs daily before the metas to keep filtered_coddoc files fresh.
# Can also be run standalone: python utils/create_filtered_assunto_parquets.py

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("meta_orchestrator")

# Runtime detection: when running under Airflow we must avoid in-process runs because
# a stopped SparkContext in one meta can poison the process for subsequent metas.
# Allow operators to override via FORCE_INPROCESS or FORCE_SUBPROCESS env vars.
try:
    force_sub = os.environ.get("FORCE_SUBPROCESS", os.environ.get("FORCE_SUBPROCESS_RUN", "")).lower()
    force_in = os.environ.get("FORCE_INPROCESS", os.environ.get("FORCE_INPROCESS_RUN", "")).lower()
    if force_sub in ("1", "true", "yes"):
        INPROCESS_CAPABLE = False
        logger.info("FORCE_SUBPROCESS set - running metas in subprocess mode to isolate SparkSessions")
    elif force_in in ("1", "true", "yes"):
        INPROCESS_CAPABLE = True
        logger.info("FORCE_INPROCESS set - allowing in-process meta runs (use with caution)")
    else:
        # Heuristic: detect Airflow by common env vars and disable in-process runs there
        if os.environ.get("AIRFLOW_CTX_DAG_ID") or os.environ.get("AIRFLOW_HOME") or any(k.startswith("AIRFLOW__") for k in os.environ):
            INPROCESS_CAPABLE = False
            logger.info("Detected Airflow environment - disabling in-process meta runs to avoid SparkContext reuse across metas")
except Exception:
    # Non-fatal; keep existing INPROCESS_CAPABLE value
    pass


def run_script(script_path: str) -> bool:
    if not os.path.exists(script_path):
        logger.error("Script not found: %s", script_path)
        return False
    logger.info("Running: %s", script_path)
    # Try to run in-process when possible; this allows meta_cnj2026 to control
    # SparkSession lifetime per script and avoid leaking JVM heap across runs.
    if INPROCESS_CAPABLE:
        # Convert path like "utils/create_filtered_assunto_parquets.py" → "utils.create_filtered_assunto_parquets"
        module_name = ".".join(Path(script_path).with_suffix("").parts)
        try:
            # Import the module dynamically and call its run_meta entrypoint if present
            module = importlib.import_module(module_name)
            if hasattr(module, "run_meta"):
                logger.info(f"🏃 Running {module_name}.run_meta() in-process")
                # Prepare shared resources for the in-process run
                try:
                    config_manager = MetaConfigManager()
                    meta_logger = MetaLogger(module_name)
                    # Create a dedicated SparkSession for this module
                    # Ensure in-process sessions use safe defaults to avoid broadcast OOMs
                    builder = SparkMetaUtils.get_optimized_spark_config(module_name)
                    try:
                        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", os.environ.get("SPARK_AUTO_BROADCAST_THRESHOLD", "-1"))
                        # Allow override via SPARK_DRIVER_MEMORY env var, default to 12g
                        builder = builder.config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "12g"))
                        logger.info(f"Starting in-process SparkSession with autoBroadcast={os.environ.get('SPARK_AUTO_BROADCAST_THRESHOLD','-1')} driver.memory={os.environ.get('SPARK_DRIVER_MEMORY','12g')}")
                    except Exception:
                        logger.debug("Could not set extra builder configs; proceeding with defaults")
                    spark = builder.getOrCreate()
                    # Enforce runtime confs on the active SparkSession to absolutely
                    # prevent any broadcast join attempts or adaptive plans.
                    try:
                        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", os.environ.get("SPARK_AUTO_BROADCAST_THRESHOLD", "-1"))
                        spark.conf.set(
                            "spark.sql.adaptive.enabled",
                            os.environ.get("SPARK_ADAPTIVE_ENABLED", os.environ.get("SPARK_AQE_ENABLED", "false"))
                        )
                        logger.info(f"Applied runtime Spark configs: autoBroadcast={spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}, adaptive={spark.conf.get('spark.sql.adaptive.enabled')}")
                    except Exception:
                        logger.debug("Could not apply runtime Spark configs on session; continuing")
                    # Quick diagnostic: check parquet path visibility (helps debug PATH_NOT_FOUND errors)
                    try:
                        from utils.parquet_utils import meta_export_path, path_exists, get_environment_info
                        sample_meta = module_name.split('_')[-1]
                        sample_path = meta_export_path(sample_meta)
                        logger.info(f"Parquet environment info: {get_environment_info()}")
                        diag = path_exists(sample_path, spark)
                        logger.info(f"Path diagnostic for {sample_path}: {diag}")
                    except Exception as _diag_e:
                        logger.debug(f"Parquet path diagnostic failed: {_diag_e}")
                    hybrid_loader = HybridParquetLoader(spark, meta_logger.logger, MetaMiscellaneous(spark, meta_logger.logger))
                    misc = MetaMiscellaneous(spark, meta_logger.logger)
                    results_exporter = MetaResultsExporter(spark, module_name.split('_')[-1], config_manager=config_manager)
                except Exception as ctx_e:
                    logger.exception(f"Failed to prepare in-process context for {module_name}: {ctx_e}")
                    # fallback to subprocess if we can't prepare context
                    raise

                try:
                    success = bool(module.run_meta(
                        spark=spark,
                        config_manager=config_manager,
                        logger=meta_logger.logger,
                        misc=misc,
                        hybrid_loader=hybrid_loader,
                        results_exporter=results_exporter,
                    ))
                    return success
                except Exception as run_e:
                    logger.exception(f"Exception running {module_name}.run_meta(): {run_e}")
                    return False
                finally:
                    # Ensure the SparkSession created for this in-process run is stopped.
                    # Wait for active jobs to finish (up to wait_timeout seconds) to avoid
                    # RejectedExecutionException when tasks are still reporting status.
                    try:
                        safe_stop_spark(spark, logger=logger, wait_for_jobs=True, wait_timeout=120)
                    except Exception:
                        pass

        except Exception:
            logger.exception(f"Falling back to subprocess for {script_path}")
            # continue to subprocess fallback
    try:
        # Run with increased timeout and better error handling for Spark cleanup
        # When running via subprocess (e.g., Airflow), ensure subprocess
        # SparkSessions are configured to avoid broadcast OOMs by default.
        env = os.environ.copy()
        # If not already provided, disable broadcast joins to be memory-safe
        # for large joins during production runs. Users can override this by
        # setting PYSPARK_SUBMIT_ARGS in their operator/environment.
        if "PYSPARK_SUBMIT_ARGS" not in env:
            # Provide a conservative default: allocate 12g driver memory and
            # disable broadcast joins to avoid OOMs on large joins in prod.
            env["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 12g --conf spark.sql.autoBroadcastJoinThreshold=-1 pyspark-shell"
            logger.info("Set PYSPARK_SUBMIT_ARGS to disable broadcast joins and set default driver memory=12g for subprocess runs")

        result = subprocess.run(
            [sys.executable, script_path], 
            cwd=os.getcwd(), 
            env=env,
            timeout=7200  # 2 hour timeout for large meta calculations
        )
        if result.returncode == 0:
            logger.info("✅ Completed: %s", script_path)
            return True
        else:
            logger.error("❌ Failed (%s): %s", result.returncode, script_path)
            return False
    except subprocess.TimeoutExpired:
        logger.error("⏰ Timeout expired for %s", script_path)
        return False
    except Exception as e:
        logger.exception("💥 Exception running %s: %s", script_path, e)
        return False

def run_assunto_generator():     
    scripts = [
        os.path.join("utils", "create_filtered_assunto_parquets.py"),
        os.path.join("utils", "create_filtered_eproc_parquets.py"),
    ]
    overall_ok = True
    for script in scripts:
        try:
            logger.info("Running helper: %s", script)
            ok = run_script(script)
            if not ok:
                logger.warning("Helper returned non-zero exit: %s", script)
            overall_ok = overall_ok and ok
        except Exception:
            logger.exception("Exception while running helper (ignored): %s", script)
            overall_ok = False
    return overall_ok

def main():
    default_spark_local = "/opt/parquet/tempspark"
    if not os.environ.get("SPARK_LOCAL_DIRS"):
        logger.warning(f"SPARK_LOCAL_DIRS not set; defaulting to {default_spark_local}. Ensure this path exists on all workers.")
        os.environ.setdefault("SPARK_LOCAL_DIRS", default_spark_local)
        try:
            os.makedirs(default_spark_local, exist_ok=True)
        except Exception:
            logger.debug("Could not create local Spark dir on driver; ensure path exists on worker nodes.")
    overall_ok = True

    logger.info("🔄 Regenerating filtered parquet files before metas...")
    run_assunto_generator()

    try:
        for i, (script, name) in enumerate(META_SCRIPTS):
            logger.info(f"🚀 Starting Meta {name} ({i+1}/{len(META_SCRIPTS)})")

            ok = run_script(script)
            overall_ok = overall_ok and ok

            if ok:
                logger.info(f"✅ Meta {name} completed")
            else:
                logger.error(f"❌ Meta {name} failed")

    except Exception as e:
        logger.exception("💥 Exception during meta calculations: %s", e)
        overall_ok = False
        exit_code = 1
    finally:
        # Best-effort cleanup to avoid leftover shuffle races
        try:
            import time
            gc.collect()
            # Slightly longer pause to allow background Spark threads to settle
            time.sleep(3)
        except Exception:
            pass
        # Exit with non-zero code when any meta failed so orchestrators (Airflow)
        # correctly detect failure. `overall_ok` is True only when all metas succeeded.
        try:
            exit_code = 0 if overall_ok else 1
        except Exception:
            exit_code = 1
        sys.exit(exit_code)



if __name__ == "__main__":
    main()