"""SparkSession with the Iceberg Glue catalog."""

from pyspark.sql import SparkSession

from eproc_metacnj.config import Settings


def create_spark_session(
    settings: Settings,
    executor_memory: str = "12g",
    executor_memory_overhead: str = "2g",
    shuffle_partitions: int = 200,
) -> SparkSession:
    cat = f"spark.sql.catalog.{settings.catalog}"
    return (
        SparkSession.builder
        .appName("EprocMetaCNJ_M01")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{cat}.warehouse", f"{settings.warehouse}/warehouse")
        .config("spark.sql.defaultCatalog", settings.catalog)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
