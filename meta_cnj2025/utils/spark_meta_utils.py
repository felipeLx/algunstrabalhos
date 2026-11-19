#!/usr/bin/env python3
"""
Spark Meta Utilities

Provides conservative defaults for repartitioning and a lightweight
helper to build an optimized SparkSession builder with safe defaults.

This file intentionally keeps default partition counts low to reduce
task/ thread pressure on small or constrained environments.
"""
from typing import Dict
import os

DEFAULT_REPARTITION_COUNT = 100  # conservative default for repartition(...)
DEFAULT_REDUCED_PARTITION_COUNT = max(16, DEFAULT_REPARTITION_COUNT // 4)


def get_optimized_spark_config(app_name: str):
    """Return a SparkSession.builder pre-configured with conservative defaults.

    The returned object is a SparkSession.builder (not yet getOrCreate) so callers
    can still chain additional .config() calls before creating the session.
    """
    try:
        from pyspark.sql import SparkSession
    except Exception:
        # If pyspark not available in this environment, raise so callers know.
        raise

    spark_local = os.environ.get('SPARK_LOCAL_DIRS') or os.environ.get('SPARK_TMP_ROOT') or '/tmp/spark-temp'
    checkpoint_dir = os.environ.get('SPARK_CHECKPOINT_DIR') or '/tmp/spark-checkpoints'
    builder = SparkSession.builder \
        .appName("NetaCnj_2inst") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.task.maxFailures", "3") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.shuffle.partitions", "1200") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.scheduler.revive.interval", "100ms") \
        .config("spark.task.cpus", "1") \
        .config("spark.locality.wait", "0") \
        .config("spark.sql.execution.sortBeforeRepartition.enabled", "true") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.local.dir", spark_local) \
        .config("spark.shuffle.file.buffer", "1m") \
        .config("spark.shuffle.unsafe.file.output.buffer", "5m") \
        .config("spark.shuffle.compress", "true") \
        .config("spark.shuffle.spill.compress", "true") \
        .config("spark.shuffle.service.enabled", "false") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.cleaner.periodicGC.interval", "15s") \
        .config("spark.cleaner.referenceTracking", "true") \
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
        .config("spark.hadoop.parquet.memory.pool.ratio", "0.5") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.checkpoint.dir", checkpoint_dir) \
        .config("spark.cleaner.referenceTracking.blocking", "false")

    return builder


class SparkMetaUtils:
    """Compatibility shim used across the codebase.

    Historically some code imports SparkMetaUtils.get_optimized_spark_config.
    Provide the same API while centralizing default partition constants.
    """
    DEFAULT_REPARTITION_COUNT = DEFAULT_REPARTITION_COUNT
    DEFAULT_REDUCED_PARTITION_COUNT = DEFAULT_REDUCED_PARTITION_COUNT

    @staticmethod
    def get_optimized_spark_config(app_name: str):
        return get_optimized_spark_config(app_name)


class SparkMetaMixin:
    """Lightweight mixin to expose spark helper constants inside calculators."""
    DEFAULT_REPARTITION_COUNT = DEFAULT_REPARTITION_COUNT
    DEFAULT_REDUCED_PARTITION_COUNT = DEFAULT_REDUCED_PARTITION_COUNT
