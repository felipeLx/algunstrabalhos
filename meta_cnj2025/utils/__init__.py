# Import main classes for easy access
from .meta_results_exporter import MetaResultsExporter
from .meta_logging_system import MetaLogger
from .meta_config_manager import MetaConfigManager
from .spark_meta_utils import SparkMetaMixin, SparkMetaUtils
from .misc import MetaMiscellaneous
from .hybrid_parquet_loader import HybridParquetLoader
from .save_to_postgres import save_parquet_to_postgres_table
from .parquet_utils import (
    parquet_path,
    resolve_path_config,
    ejud_situacao_base,
    eproc_situacao_base,
    ejud_export_base,
    eproc_export_base,
    spark_events_path,
    filtered_coddoc_path,
    get_environment_info,
    ejud_bronze_base,
    movimento_path
)


__version__ = "1.0.0"
__author__ = "Meta CNJ 2026 Team"

# Package-level configuration
DEFAULT_CONFIG = {
    "export_base_path": "/opt/parquet/data/prata",
    "log_level": "INFO",
    "spark_optimization": True,
    "hybrid_loading": True
}

# Export public API
__all__ = [
    "MetaResultsExporter",
    "MetaLogger", 
    "MetaMiscellaneous",
    "MetaConfigManager",
    "SparkMetaMixin",
    "SparkMetaUtils",
    "HybridParquetLoader",
    "parquet_path",
    "resolve_path_config",
    "ejud_situacao_base",
    "eproc_situacao_base",
    "ejud_export_base",
    "eproc_export_base",
    "spark_events_path",
    "save_parquet_to_postgres_table",
    "filtered_coddoc_path",
    "movimento_path",
    "get_environment_info",
    "ejud_bronze_base",
    "DEFAULT_CONFIG",
]

# Backwards compatibility: gracefully provide no-op stubs if legacy imports occur
def __getattr__(name):
    if name in {"MetaScheduler", "SchedulingStrategy", "calculate_suspension_snapshot", "calculate_unsuspension_events", "ArrowOptimizationFix"}:
        raise AttributeError(f"{name} has been removed from the trimmed production utils package.")
    raise AttributeError(f"module 'utils' has no attribute '{name}'")
