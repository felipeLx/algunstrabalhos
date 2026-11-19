#!/usr/bin/env python3
import logging
import os
import sys
import time
from datetime import datetime, date
from pyspark.sql import SparkSession
from dotenv import load_dotenv
PARQUET_HOME = f"/opt/parquet"
load_dotenv(dotenv_path=os.path.join(PARQUET_HOME, "airflow", "dotenv", ".env"),  override=True)
sys.path.append(os.path.join(PARQUET_HOME, "projetos"))
sys.path.insert(0, os.path.join(PARQUET_HOME, "projetos"))

from modulo.controle_carga import (
    registrar_status_carga,
    registrar_inicio_carga,
    registrar_fim_carga)
from modulo.conexoes import get_postgres_connection
_HAS_CONTROLE_CARGA= True


def wait_for_spark_completion(spark, logger, timeout_seconds=30):
    """
    Wait for all Spark operations to complete before shutdown
    """
    logger.info("🔄 Ensuring all Spark operations are complete...")
    
    try:
        # Execute a simple operation to force completion of any lazy operations
        test_df = spark.sql("SELECT 1 as test_completion")
        test_df.collect()
        
        # Check for active jobs
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            active_jobs = spark.sparkContext.statusTracker().getActiveJobIds()
            if not active_jobs:
                logger.info("✅ No active Spark jobs detected")
                break
            else:
                logger.info(f"⏳ Waiting for {len(active_jobs)} active jobs to complete...")
                time.sleep(2)
        
        # Final wait to ensure async cleanup
        time.sleep(5)
        logger.info("✅ Spark completion check finished")
        
    except Exception as e:
        logger.warning(f"⚠️ Spark completion check warning: {e}")
        # Still wait a bit even if check fails
        time.sleep(5)

def run_all_metas():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("meta_orchestrator")

    # Avoid spilling/shuffle/blockmgr temp files to /tmp (often small in Airflow workers/containers)
    # Prefer an externally mounted directory with enough space.
    spark_local_dirs = (
        os.environ.get("SPARK_LOCAL_DIRS")
        or os.environ.get("SPARK_LOCAL_DIR")
        or os.environ.get("SPARK_TMP_DIR")
        or "/opt/parquet/tmp/spark_local"
    )
    try:
        # Support comma-separated multiple dirs
        for d in [p.strip() for p in spark_local_dirs.split(",") if p.strip()]:
            os.makedirs(d, exist_ok=True)
        logger.info(f"Using Spark local dirs (spill/shuffle): {spark_local_dirs}")
    except Exception as e:
        logger.warning(f"Could not create Spark local dirs '{spark_local_dirs}': {e}. Spark may fall back to /tmp")
    
    # IMPROVED: Configure Spark session with optimized temporary storage and shuffle settings
    spark = SparkSession.builder \
        .appName("MetaPipeline") \
        .config("spark.local.dir", spark_local_dirs) \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.executor.instances", "2") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.execution.arrow.enabled", "false") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.ui.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer", "64m") \
        .config("spark.kryoserializer.buffer.max", "1g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.default.parallelism", "32") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.scheduler.revive.interval", "100ms") \
        .config("spark.task.cpus", "1") \
        .config("spark.locality.wait", "0") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.shuffle.compress", "true") \
        .config("spark.shuffle.spill.compress", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.cleaner.periodicGC.interval", "30s") \
        .config("spark.cleaner.referenceTracking", "true") \
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
        .config("spark.checkpoint.dir", "/opt/parquet/tmp_felipe") \
        .config("spark.sparkContext.checkpointDir", "/opt/parquet/tmp_felipe") \
        .config("spark.memory.storageFraction", "0.3") \
        .getOrCreate()
            
    # Set log level to reduce noise during cleanup
    spark.sparkContext.setLogLevel("ERROR")  # More aggressive - only show errors

    # Import calculator classes and config manager
    from utils.meta_config_manager import MetaConfigManager
    from meta_calculator import MetaCalculator
    from meta_01_eproc import EprocMetaCalculator
    from pathlib import Path

    id_projeto = 4
    schema_name = "dw2inst"
    tabela_name = "meta_analitico"
    home_executor = str(Path(__file__).resolve())
    
    start_time = datetime.now()
    dt_referencia = date.today()
    if _HAS_CONTROLE_CARGA:
        try:
            # registrar_inicio_carga: include dt_carga kw for start timestamp
            #registrar_inicio_carga(id_projeto, schema_name, tabela_name, dt_referencia, home_executor, dt_carga=start_time)
            pass
        except Exception:
            logger.exception("registrar_inicio_carga failed")

    config_mgr = MetaConfigManager()
    def is_empty_data_error(exc):
        try:
            msg = str(exc or "")
            cls = exc.__class__.__name__ if exc is not None else ""
            keywords = [
                "no data",
                "empty",
                "no rows",
                "path does not exist",
                "empty parquet",
                "nosuchfile",
                "not exist",
                "filenotfound",
            ]
            if any(k in msg.lower() for k in keywords):
                return True
            if cls in ("FileNotFoundError", "NoSuchFileException", "EmptyDataFrame"):
                return True
            # Py4J/Analysis exceptions may include Java-side messages
            if cls in ("AnalysisException", "Py4JJavaError"):
                if "Path does not exist" in msg or "java.io.FileNotFoundException" in msg or "NoSuchFileException" in msg:
                    return True
            return False
        except Exception:
            return False
    
    # STRICTLY SEQUENTIAL PROCESSING - DO NOT PARALLELIZE    
    metas = ["01", "02", "04", "06", "07", "08", "10", "11","01_eproc"]
    overall_ok = True

    # -----------------------------------------------------------------------
    # PRÉ-PIPELINE: limpa fato_auxiliar para o ano alvo antes de qualquer meta
    # Evita duplicatas em re-execuções do pipeline (o maior vetor de duplicatas).
    # -----------------------------------------------------------------------
    _target_year_pipeline = 2026
    try:
        _pg_conn_pre = get_postgres_connection()
        _pg_conn_pre.autocommit = True
        with _pg_conn_pre.cursor() as _cur_pre:
            _cur_pre.execute(
                f"DELETE FROM dw2inst.fato_auxiliar WHERE meta_ano = {_target_year_pipeline}"
            )
            deleted_rows = _cur_pre.rowcount
        _pg_conn_pre.close()
        logger.info(
            f"Pré-pipeline: {deleted_rows:,} linhas removidas de dw2inst.fato_auxiliar "
            f"para meta_ano={_target_year_pipeline} — tabela limpa para nova carga."
        )
    except Exception as _pre_err:
        logger.error(
            f"Pré-pipeline DELETE em fato_auxiliar falhou: {_pre_err}. "
            f"Abortando para não gerar duplicatas."
        )
        raise

    logger.info("🚀 Starting SEQUENTIAL meta processing...")
    logger.info(f"📋 Processing order: {' -> '.join(metas)}")
    
    for i, meta in enumerate(metas):
        
        calc_name = f"Meta {meta}"
        execution_step = f"Step {i+1}/{len(metas)}"

        logger.info("="*60)
        logger.info(f"🔄 {execution_step}: Starting {calc_name}")
        logger.info("="*60)

        try:
            mode = "append" if meta == "01" else "append"
            logger.info(f"📝 Mode: {mode}")

            # CRITICAL: Clear Spark cache and temporary files before each meta to avoid stale file references
            logger.info("🧹 Comprehensive cleanup before meta processing...")
            try:
                spark.catalog.clearCache()

                # Attempt to clean local temp dirs only when safe
                try:
                    import shutil
                    env_local_dirs = os.environ.get("SPARK_LOCAL_DIRS") or os.environ.get("LOCAL_DIRS")
                    configured_local_dir = None
                    try:
                        configured_local_dir = spark.conf.get("spark.local.dir")
                    except Exception:
                        configured_local_dir = None

                    temp_dirs = []
                    if env_local_dirs:
                        logger.warning(
                            f"⚠️ Detected cluster-configured local dirs via environment: {env_local_dirs}. Will NOT delete those dirs while Spark is running to avoid removing active shuffle/block files."
                        )
                    else:
                        if configured_local_dir and isinstance(configured_local_dir, str) and configured_local_dir.startswith("/tmp"):
                            temp_dirs.append(configured_local_dir)
                        else:
                            if configured_local_dir:
                                logger.warning(f"⚠️ spark.local.dir is set to {configured_local_dir}. Will not attempt to clean it while Spark is running.")
                            else:
                                temp_dirs.append("/tmp/spark-temp")

                    for temp_dir in temp_dirs:
                        if os.path.exists(temp_dir):
                            try:
                                for item in os.listdir(temp_dir):
                                    item_path = os.path.join(temp_dir, item)
                                    if os.path.isdir(item_path):
                                        shutil.rmtree(item_path)
                                    else:
                                        os.remove(item_path)
                                logger.info(f"✅ Cleaned Spark temp directory: {temp_dir}")
                            except Exception as clean_error:
                                logger.warning(f"⚠️ Could not clean {temp_dir}: {clean_error}")
                except Exception as inner_clean_err:
                    logger.warning(f"⚠️ Error during local dir cleanup: {inner_clean_err}")

                try:
                    spark.sql("CLEAR CACHE")
                except Exception as clear_cache_err:
                    logger.warning(f"⚠️ Error issuing CLEAR CACHE: {clear_cache_err}")
            except Exception as temp_clean_error:
                logger.warning(f"⚠️ Temporary directory cleanup warning: {temp_clean_error}")

            try:
                spark.sql("REFRESH TABLE IF EXISTS default.parquet_cache")
            except Exception:
                pass

            logger.info("✅ Comprehensive cleanup completed successfully")
        except Exception as cache_error:
            logger.warning(f"⚠️ Cache clearing warning: {cache_error}")

        import gc
        import time
        gc.collect()
        logger.info("🧹 Memory cleanup completed")

        try:
            # Use EprocMetaCalculator for EPROC metas, MetaCalculator for EJUD metas
            if meta == "01_eproc":
                calc = EprocMetaCalculator(spark=spark, logger=logger, config_mgr=config_mgr, meta_number=meta, mode=mode)
            else:
                calc = MetaCalculator(spark=spark, logger=logger, config_mgr=config_mgr, meta_number=meta, mode=mode)

            logger.info(f"⚡ Executing {calc_name} normalize_and_save()...")
            try:
                success = calc.normalize_and_save()
            except Exception as e:
                if is_empty_data_error(e):
                    logger.warning(f"⚠️ {calc_name} produced no data or missing input; skipping meta. Details: {e}")
                    del calc
                    gc.collect()
                    logger.info(f"📊 Remaining metas: {len(metas) - (i+1)}")
                    continue
                raise

            if not success:
                logger.warning(f"⚠️ {calc_name} returned False (likely no data). Skipping and continuing.")
                del calc
                gc.collect()
                logger.info(f"📊 Remaining metas: {len(metas) - (i+1)}")
                continue

            del calc
            gc.collect()

            logger.info("⏳ Final wait for Spark task completion...")
            time.sleep(5)

            logger.info(f"✅ {execution_step}: {calc_name} completed successfully")
            logger.info(f"📊 Remaining metas: {len(metas) - (i+1)}")
        except Exception as e:
            logger.error(f"❌ {execution_step}: {calc_name} FAILED!")
            logger.exception(f"💥 Error details: {e}")
            overall_ok = False
            logger.error("🛑 Stopping processing due to failure (maintaining data integrity)")
            break
    
    end_time = datetime.now()
    tempo_execucao = end_time - start_time
    qt_registros = None
    try:
        logger.info(f"Start to stablish connection")
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {schema_name}.{tabela_name}"
            )
            qt_registros = cur.fetchone()[0]
        conn.close()
        logger.info(f"Finished with success")
    except:
        logger.info(f"Finished with exception")
        pass

    # Final status report
    logger.info("="*60)
    if overall_ok:
        #registrar_status_carga(schema_name, tabela_name, 'SUCESSO', None)
        #registrar_fim_carga(id_projeto=id_projeto, schema=schema_name, tabela=tabela_name, dt_referencia=dt_referencia, dt_fim_carga=end_time, qt_registros=qt_registros, status='SUCESSO', tempo_execucao=tempo_execucao, mensagem_erro=None)
        logger.info("✅ ALL METAS COMPLETED SUCCESSFULLY!")
        logger.info("🎉 Meta analytics pipeline finished without errors")
        logger.info(f"📋 Processed metas: {' ✓ '.join(metas)}")
        sys.exit(0)
    else:
        #registrar_fim_carga(id_projeto=id_projeto,schema=schema_name,tabela=tabela_name,dt_referencia=dt_referencia,dt_fim_carga=end_time,qt_registros=qt_registros,status='FALHA',tempo_execucao=tempo_execucao,mensagem_erro=f"Problemas ocorreram durante a execução das metas.")
        logger.error("❌ PIPELINE FAILED!")
        logger.error("⚠️  Some metas failed - check logs above for details")
        logger.error("🔧 Fix issues and rerun the pipeline")
        sys.exit(0)

if __name__ == "__main__":
    run_all_metas()