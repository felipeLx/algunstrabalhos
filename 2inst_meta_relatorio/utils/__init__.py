"""
Utilities module for situacao_incremental project
Contains helper functions for data processing and database operations
"""

from .save_to_postgres import save_parquet_to_postgres
from .meta_normalization  import standardize_combined

# --- CHANGE 1: Import the class here ---
from .facto_auxiliar import FactoAuxiliar 

from .parquet_utils import (
    parquet_path, get_storage_info, configure_spark_for_storage,
    # Bronze paths
    ejud_bronze_t_processo, ejud_bronze_tipoprocesso, ejud_bronze_tab_xml, ejud_bronze_movimentorelator,
    ejud_bronze_processoassunto, ejud_bronze_assunto, eproc_bronze_processo,
    ejud_bronze_orgao,
    meta_02_current,
    meta_current, get_ultimadistribuicaoriger_path,
    # Silver paths  
    ejud_silver, ejud_initial, ejud_filled, eproc_silver, meta_export_path,
    # Gold paths
    meta_01_analytics_ejud, meta_01_analytics_eproc, meta_02_analytics_ejud,
    meta_02_analytics_eproc, combined_meta_analytics,
    # Builders
    get_meta_analytics_path, get_bronze_path, get_silver_path
)

__all__ = [
    # Core modules
    'save_parquet_to_postgres',
    'standardize_combined',
    'FactoAuxiliar', # --- CHANGE 2: Add to __all__ list ---
    
    # Path utilities
    'parquet_path', 'get_storage_info', 'configure_spark_for_storage',
    # Bronze paths
    'ejud_bronze_t_processo', 'ejud_bronze_tab_xml', 'get_ultimadistribuicaoriger_path', 'ejud_bronze_tipoprocesso', 'ejud_bronze_movimentorelator',
    'ejud_bronze_processoassunto', 'ejud_bronze_assunto', 
    'meta_02_current',
    'meta_current',
    'eproc_bronze_processo',
    'ejud_bronze_orgao',
    # Silver paths
    'ejud_silver', 'ejud_initial', 'ejud_filled', 'eproc_silver', 'meta_export_path',
    # Gold paths
    'meta_01_analytics_ejud', 
    'meta_01_analytics_eproc', 'meta_02_analytics_ejud',
    'meta_02_analytics_eproc', 'combined_meta_analytics',
    # Builders
    'get_meta_analytics_path', 'get_bronze_path', 'get_silver_path'
]