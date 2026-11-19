import os
import logging
import glob
import gc
import shutil
import pandas as _pd
import json
import time
from contextlib import contextmanager
import traceback

# PySpark imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, sum as spark_sum,
        max as spark_max, min as spark_min, first, desc, asc,
        year, month, dayofmonth, to_date, date_format,
        broadcast, coalesce, current_timestamp, concat_ws, 
        row_number, rank, dense_rank
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        TimestampType, DateType, LongType, DoubleType
    )
    from pyspark import StorageLevel
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

class MetaMiscellaneous:

    def __init__(self, spark, logger, meta_number: str = "04"):
        """
        Misc utilities container.

        Args:
            spark: Spark session
            logger: optional external logger (if provided, will be used instead of MetaLogger)
            meta_number: meta number used when creating an internal MetaLogger (default '04')
        """
        self.spark = spark
        if logger is not None:
            self.logger = logger
            # create a lightweight meta_logger wrapper to keep API consistent
            try:
                self.meta_logger = MetaLogger(meta_number)
            except Exception:
                # If MetaLogger construction fails, provide a minimal placeholder
                class _StubMetaLogger:
                    def __init__(self, l):
                        self.logger = l
                    def log_variable_completion(self, *a, **k):
                        pass
                self.meta_logger = _StubMetaLogger(self.logger)
        else:
            self.meta_logger = MetaLogger(meta_number)
            self.logger = self.meta_logger.logger

    def _create_empty_result_df(self):
        """Create an empty DataFrame with the standard result schema for Meta 02."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
        
        empty_schema = StructType([
            StructField("coddoc", LongType(), True),
            StructField("dthrmov", TimestampType(), True),
            StructField("dth_fim", TimestampType(), True),
            StructField("situacao", IntegerType(), True),
            StructField("variavel", StringType(), True)
        ])
        return self.spark.createDataFrame([], empty_schema)
    
    def _load_constituent_variable_from_parquet(self, parquet_path: str, variable_name: str) -> DataFrame:
        """Load constituent variable from exported parquet file."""
        try:
            try:
                self.logger.debug("Clearing Spark catalog and SQL cache before reading processoassunto")
                try:
                    self._cleanup_memory_aggressive()
                    self.spark.catalog.clearCache()
                except Exception:
                    pass
                try:
                    self.spark.sql("CLEAR CACHE")
                except Exception:
                    pass
                try:
                    # Best-effort REFRESH for the parquet path to update metadata
                    self.spark.sql(f"REFRESH TABLE parquet.`{parquet_path}`")
                except Exception:
                    # REFRESH may fail if path isn't a registered table; ignore and continue
                    pass
            except Exception:
                # If cache invalidation fails, continue and let the read attempt proceed
                self.logger.debug("Could not fully invalidate Spark caches prior to processoassunto read")

            self.logger.info(f"📂 Loading {variable_name} from: {parquet_path}")
            df = self.spark.read.parquet(parquet_path)
            return df
                
        except Exception as e:
            self.logger.error(f"❌ Error loading {variable_name}: {e}")
            return self._create_empty_result_df()

    def _cleanup_memory_aggressive(self, stop_spark=False):
        try:
            # Force Python garbage collection to release DataFrame references
            import gc
            gc.collect()
                
            # Only stop Spark session if explicitly requested (final cleanup)
            if stop_spark:
                try:
                    if self.spark:
                        self.spark.stop()
                        self.spark = None
                except Exception:
                    pass  # Ignore shutdown errors
                
            self.logger.debug(f"🧹 Memory cleanup completed {'with Spark stop' if stop_spark else 'without Spark stop'}")
        except Exception as e:
            # Don't log cleanup errors as they're normal during shutdown
            pass

    def _persist_dataframe_safely(self, df: DataFrame, storage_level=None, partition_by=None) -> DataFrame:
        """Safely persist DataFrame with memory management.
        """
        if storage_level is None:
            storage_level = StorageLevel.MEMORY_AND_DISK

        # Normalize partition_by to list
        if isinstance(partition_by, str):
            partition_by = [partition_by]
        df_copy = df
        try:
            partition_ = 200
            # Repartition by column(s) if requested, otherwise evenly
            if partition_by:
                df_repartitioned = df_copy.repartition(partition_, *partition_by)
            else:
                df_repartitioned = df_copy.repartition(partition_)

            df_persisted = df_repartitioned.persist(storage_level)


            self.logger.debug(f"📊 Persisted DataFrame records across partitions (partition_by={partition_by})")

            return df_persisted

        except Exception as e:
            self.logger.warning(f"⚠️ Error persisting DataFrame: {e}")
            return self._create_empty_result_df()

    def _parquet_has_data(self, path: str) -> bool:
        """Check whether a parquet directory or file contains any data files."""
        try:
            if not path or not os.path.exists(path):
                return False

            if os.path.isfile(path):
                return os.path.getsize(path) > 0

            for root, _, files in os.walk(path):
                for fname in files:
                    if fname.startswith("_"):
                        continue
                    full_path = os.path.join(root, fname)
                    try:
                        if os.path.getsize(full_path) > 0:
                            return True
                    except OSError:
                        continue
            return False
        except Exception as e:
            self.logger.debug(f"DEBUG: Failed parquet inspection at {path}: {e}")
            return False

def safe_stop_spark(spark, logger: logging.Logger = None, pause_seconds: float = 1.0,
                    wait_for_jobs: bool = True, wait_timeout: int = 60):
    """Graceful stop for a SparkSession with optional wait for running jobs.

    This helper will:
    - try to wait for active Spark jobs to finish (configurable timeout)
    - if timeout expires, attempt to cancel jobs before stopping
    - call SparkSession.stop() and swallow shutdown errors

    Args:
        spark: SparkSession-like object
        logger: optional logger to report progress
        pause_seconds: seconds to sleep after stop (small grace)
        wait_for_jobs: whether to wait for active jobs to finish before stopping
        wait_timeout: total seconds to wait for active jobs before forcing cancel
    """
    try:
        gc.collect()
    except Exception:
        pass

    # Debugging aid: log a short stack trace whenever someone requests a stop.
    # This helps identify which module or code path called stop() in Airflow runs.
    try:
        stack = "".join(traceback.format_stack(limit=20))
        if logger is not None:
            try:
                logger.warning("safe_stop_spark called - stack (most recent call last):\n%s", stack)
            except Exception:
                # Fallback to printing if logger fails
                print("safe_stop_spark called - stack:\n" + stack)
        else:
            # If no logger provided, print to stdout so Airflow captures it
            print("safe_stop_spark called - stack:\n" + stack)
    except Exception:
        # Swallow any error in debugging/logging to avoid interfering with shutdown
        pass

    if not spark:
        try:
            if logger is not None:
                logger.debug("safe_stop_spark called with None spark")
        except Exception:
            pass
        return

    # Optionally wait for active jobs to finish to avoid race conditions where
    # the Spark driver shuts down while background tasks are still reporting status.
    try:
        sc = None
        try:
            sc = spark.sparkContext
        except Exception:
            sc = None

        if wait_for_jobs and sc is not None:
            try:
                tracker = sc.statusTracker()
                waited = 0
                poll_interval = 1
                active = tracker.getActiveJobIds()
                # convert to length safely
                active_ = len(active) if active is not None else 0
                while active_ > 0 and waited < wait_timeout:
                    if logger is not None:
                        try:
                            logger.info(f"Waiting for {active_} active Spark job(s) to finish (waited {waited}s)")
                        except Exception:
                            pass
                    time.sleep(poll_interval)
                    waited += poll_interval
                    try:
                        active = tracker.getActiveJobIds()
                        active_ = len(active) if active is not None else 0
                    except Exception:
                        break

                if active_ > 0:
                    # Timeout expired - attempt a safe cancel of running jobs before stopping
                    try:
                        if logger is not None:
                            logger.warning(f"Timeout waiting for active Spark jobs ({active_} remaining). Attempting cancelAllJobs() before stop.")
                        sc.cancelAllJobs()
                        # give a short grace after cancellation
                        time.sleep(1)
                    except Exception as e:
                        if logger is not None:
                            logger.warning(f"Failed to cancel active Spark jobs: {e}")
            except Exception:
                # If statusTracker isn't available or fails, continue to stop
                pass

    except Exception:
        # Any failure in the waiting logic should not prevent shutdown
        pass

    # Finally, attempt to stop the session
    try:
        try:
            spark.stop()
            if logger is not None:
                try:
                    logger.info("Spark session stopped gracefully.")
                except Exception:
                    pass
        except Exception:
            if logger is not None:
                try:
                    logger.warning("Failed to stop Spark gracefully.")
                except Exception:
                    pass
    except Exception:
        # Swallow any error during stop - this is best-effort
        pass

    try:
        time.sleep(pause_seconds)
    except Exception:
        pass

