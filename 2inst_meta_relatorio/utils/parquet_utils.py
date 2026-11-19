import os
import logging
from pathlib import Path
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# Environment-driven configuration for parquet locations
PARQUET_HOME = os.environ.get('PARQUET_HOME', '/opt/parquet/data')
AMBIENTE = os.environ.get('AMBIENTE', 'local').lower()
S3A_BUCKET = os.environ.get('S3A_BUCKET', 'sgdai-data-lake')
S3A_ENDPOINT = os.environ.get('S3A_ENDPOINT', '')  # Optional custom S3 endpoint

# Path validation and logging
logger.info(f"Parquet Utils Configuration:")
logger.info(f"  AMBIENTE: {AMBIENTE}")
logger.info(f"  PARQUET_HOME: {PARQUET_HOME}")
if AMBIENTE != 'local':
    logger.info(f"  S3A_BUCKET: {S3A_BUCKET}")
    if S3A_ENDPOINT:
        logger.info(f"  S3A_ENDPOINT: {S3A_ENDPOINT}")

def resolve_path_config(config_path: str) -> str:
    """Resolve a path configuration value to an absolute path.

    If config_path starts with '/', treats it as absolute.
    Otherwise, treats it as relative to PARQUET_HOME and resolves via parquet_path().
    """
    if config_path.startswith('/'):
        # Absolute path - return as-is for local, or convert to s3a:// for S3
        if AMBIENTE == 'local':
            return config_path
        else:
            # Convert absolute path to relative by removing PARQUET_HOME prefix
            if config_path.startswith(PARQUET_HOME):
                rel_path = config_path[len(PARQUET_HOME):].lstrip('/\\')
                return f"s3a://{S3A_BUCKET}/{rel_path}"
            else:
                # Fallback: assume it's already relative to bucket root
                stripped_path = config_path.lstrip('/\\')
                return f"s3a://{S3A_BUCKET}/{stripped_path}"

    # Relative path - use parquet_path to resolve
    return parquet_path(config_path)

def parquet_path(rel_path: str) -> str:
    """
    Build a storage-aware path.
    - local: joins PARQUET_HOME + rel_path (so rel_path should be relative to PARQUET_HOME or start with 'data/...')
    - non-local: returns an s3a:// URI pointing to the configured bucket + rel_path
    """
    rp = rel_path.lstrip("/")

    # Avoid duplicating a leading 'data/' when PARQUET_HOME already contains '/data'
    if PARQUET_HOME.rstrip("/").endswith("/data") and rp.startswith("data/"):
        rp = rp[len("data/"):]

    if AMBIENTE == "local":
        # If rel_path is already an absolute path under PARQUET_HOME, return as-is
        abs_candidate = rp
        if not rp.startswith("/"):
            abs_candidate = os.path.join(PARQUET_HOME, rp)
        # Normalize and return absolute local path
        return os.path.normpath(abs_candidate)
    else:
        # s3 path
        return f"s3a://{S3A_BUCKET}/{rp}"


def ensure_local_directory(path: str) -> None:
    """Ensure a local directory exists (no-op for s3 paths)."""
    if path.startswith("s3a://"):
        return
    Path(path).mkdir(parents=True, exist_ok=True)


def get_storage_info() -> dict:
    return {
        "ambiente": AMBIENTE,
        "parquet_home": PARQUET_HOME,
        "s3a_bucket": S3A_BUCKET,
        "s3a_endpoint": S3A_ENDPOINT
    }


# =============================================================================
# BRONZE / SILVER / GOLD helpers (minimal set used by meta code)
# =============================================================================
def ejud_bronze_orgao():
    """EJUD bronze orgao path"""
    return parquet_path('data/bronze/ejud/orgaojulgador')

def ejud_bronze_tab_xml(): #prata
    """EJUD bronze tab_xml_andamento path"""
    return parquet_path('data/prata/ejud/tab_xml_andamento')

def ejud_bronze_tipoprocesso():
    """EJUD bronze tipoprocesso path"""
    return parquet_path('data/bronze/ejud/tipoprocesso')

def ejud_bronze_t_processo():
    """EJUD bronze t_processo path"""
    return parquet_path('data/bronze/ejud/t_processo')


def ejud_bronze_movimentorelator():
    """EJUD bronze movimentorelator path"""
    return parquet_path('data/bronze/ejud/movimentorelator')

def eproc2g_historico_distribuicao_judicial():
    """EPROC2G bronze historico_distribuicao_judicial path"""
    return parquet_path('data/bronze/eproc2g/historico_distribuicao_judicial')


# Backwards-compatible wrapper names (used by older code)
def bronze_movimentorelator():
    """Compatibility wrapper for bronze movimentorelator path"""
    return ejud_bronze_movimentorelator()


def ejud_bronze_processoassunto():
    """EJUD bronze processoassunto path"""
    return parquet_path('data/bronze/ejud/processoassunto')


def ejud_bronze_assunto():
    """EJUD bronze assunto path"""
    return parquet_path('data/bronze/ejud/assunto')


def eproc_bronze_processo():
    """EPROC bronze processo path"""
    return parquet_path('data/bronze/eproc2g/processo/processo.parquet')


def eproc_bronze_movimento():
    """EPROC bronze movimento path"""
    return parquet_path('data/bronze/eproc2g/movimento')


def ejud_bronze_competencia():
    """EJUD bronze competencia path"""
    return parquet_path('data/bronze/ejud/competencia')


# =============================================================================
# SILVER LAYER PATHS (PRATA)
# =============================================================================

def ejud_silver():
    """EJUD silver situacao_ejud path"""
    return parquet_path('data/prata/ejud/situacao_ejud')

def ejud_initial():
    """EJUD silver situacao_ejud_initial path"""
    return parquet_path('data/prata/ejud/situacao_ejud_initial')


def ejud_filled():
    """EJUD silver situacao_ejud_filled path"""
    return parquet_path('data/prata/ejud/situacao_ejud_filled')


def eproc_silver():
    """EPROC silver situacao path"""
    return parquet_path('data/prata/eproc2g/situacao_eproc')


# =============================================================================
# GOLD LAYER PATHS (OURO)
# =============================================================================

def meta_01_analytics_base():
    """Base path for Meta 01 analytics"""
    return parquet_path('data/ouro/cnj_meta2026/meta_01_analytic')


def meta_01_analytics_ejud():
    """Meta 01 analytics EJUD path"""
    return parquet_path('data/ouro/cnj_meta2026/meta_01_analytic/ejud')


def meta_01_analytics_eproc():
    """Meta 01 analytics EPROC path"""
    return parquet_path('data/ouro/cnj_meta2026/meta_01_analytic/eproc')


def meta_02_analytics_base():
    """Base path for Meta 02 analytics"""
    return parquet_path('data/ouro/cnj_meta2026/meta_02_analytic')


def meta_02_analytics_ejud():
    """Meta 02 analytics EJUD path"""
    return parquet_path('data/ouro/cnj_meta2026/meta_02_analytic/ejud')


def meta_02_analytics_eproc():
    """Meta 02 analytics EPROC path"""
    return parquet_path('data/ouro/cnj_meta2026/meta_02_analytic/eproc')


def combined_meta_analytics():
    """Combined meta analytics final path"""
    return parquet_path('data/ouro/indicadores_meta/meta_cnj2026.parquet')


# Compatibility: meta current paths expected by legacy code
def meta_current(meta_number: str = "01", sistema: str = "ejud") -> str:
    """Return current meta_01 prata folder for a given sistema"""
    return parquet_path(f"data/prata/{sistema}/meta_cnj2026/meta_{meta_number}/current")

def meta_01_current(sistema: str = "ejud") -> str:
    """Return current meta_01 prata folder for a given sistema"""
    return parquet_path(f"data/prata/{sistema}/meta_cnj2026/meta_01/current")


def meta_02_current(sistema: str = "ejud") -> str:
    """Return current meta_02 prata folder for a given sistema"""
    return parquet_path(f"data/prata/{sistema}/meta_cnj2026/meta_02/current")


def ouro_analytics_path() -> str:
    """Unified ouro analytic path for meta_analitico.parquet"""
    return parquet_path('data/ouro/cnj_meta2026/meta_analitico.parquet')


def dim_magistrado_path() -> str:
    """Compatibility path for dim_magistrado used in EPROC joins"""
    return parquet_path('data/ouro/dim2inst/dim_magistrado')


def ouro_eproc2g_acervo():
    """EPROC2G acervo gold path"""
    return parquet_path('data/ouro/eproc2g_acervo')


# =============================================================================
# SYSTEM-SPECIFIC PATH BUILDERS
# =============================================================================

def get_meta_analytics_path(meta_type: str, sistema: Optional[str] = None) -> str:
    """
    Get Meta analytics path for specific meta type and system
    
    Args:
        meta_type: "meta_01", "meta_02", etc.
        sistema: "ejud", "eproc", or None for base path
        
    Returns:
        Full parquet path for the specified meta analytics
    """
    base_path = f'data/ouro/cnj_meta2026/{meta_type}_analytic'
    
    if sistema:
        return parquet_path(f'{base_path}/{sistema}')
    else:
        return parquet_path(base_path)


def get_ultimadistribuicaoriger_path(sistema: str) -> str:
    # The `ultimadistribuicaoriger` dataset is partitioned by ano/mes/dia.
    # Return the base directory so Spark can read the partitioned layout.
    if sistema == "ejud":
        return parquet_path('data/bronze/ejud/ultimadistribuicaoriger')
    elif sistema == "eproc2g":
        return parquet_path('data/bronze/eproc2g/ultimadistribuicaoriger')
    else:
        raise ValueError(f"Unsupported sistema: {sistema}")

def get_bronze_path(sistema: str, table: str) -> str:
    """
    Get bronze layer path for specific system and table
    
    Args:
        sistema: "ejud" or "eproc2g"
        table: table/file name
        
    Returns:
        Full bronze parquet path
    """
    if sistema == "ejud":
        return parquet_path(f'data/bronze/ejud/{table}')
    elif sistema == "eproc2g":
        return parquet_path(f'data/bronze/eproc2g/{table}')
    else:
        raise ValueError(f"Unsupported sistema: {sistema}")


def get_silver_path(sistema: str, table: str) -> str:
    """
    Get silver layer path for specific system and table
    
    Args:
        sistema: "ejud" or "eproc2g" 
        table: table/file name
        
    Returns:
        Full silver parquet path
    """
    if sistema == "ejud":
        return parquet_path(f'data/prata/ejud/{table}')
    elif sistema == "eproc2g":
        return parquet_path(f'data/prata/eproc2g/{table}')
    else:
        raise ValueError(f"Unsupported sistema: {sistema}")

def meta_export_path(meta_name: str, system: str = "ejud"):
    """Path for meta export results."""
    if system.lower() == "ejud":
        return parquet_path(f'data/prata/ejud/meta_cnj2026/{meta_name}/current')
    elif system.lower() == "eproc":
        return parquet_path(f'data/prata/eproc2g/meta_cnj2026/{meta_name}/current')
    else:
        raise ValueError(f"Unknown system: {system}")

# =============================================================================
# SPARK CONFIGURATION HELPERS
# =============================================================================

def get_spark_s3_config() -> dict:
    """
    Get Spark configuration for S3 access
    
    Returns:
        Dictionary of Spark configuration properties for S3
    """
    if AMBIENTE == 'local':
        return {}
    
    config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.sql.parquet.mergeSchema": "true",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.parquet.enableVectorizedReader": "true"
    }
    
    # Add endpoint if specified (for MinIO or custom S3)
    if S3A_ENDPOINT:
        config["spark.hadoop.fs.s3a.endpoint"] = S3A_ENDPOINT
        config["spark.hadoop.fs.s3a.path.style.access"] = "true"
    
    # Add credentials from environment if available
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    if aws_access_key:
        config["spark.hadoop.fs.s3a.access.key"] = aws_access_key
    if aws_secret_key:
        config["spark.hadoop.fs.s3a.secret.key"] = aws_secret_key
    
    return config

# Environment info for logging/debugging
def get_environment_info() -> dict:
    """Get current environment configuration info."""
    return {
        'ambiente': AMBIENTE,
        'parquet_home': PARQUET_HOME,
        's3a_bucket': S3A_BUCKET,
        'is_s3': AMBIENTE != 'local'
    }


def configure_spark_for_storage(spark_session):
    """
    Configure Spark session for current storage environment
    
    Args:
        spark_session: Active Spark session to configure
    """
    if AMBIENTE != 'local':
        s3_config = get_spark_s3_config()
        for key, value in s3_config.items():
            spark_session.conf.set(key, value)
        logger.info(f"Configured Spark for S3 storage with {len(s3_config)} properties")
    else:
        logger.info("Using local storage configuration")


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def list_available_paths() -> dict:
    """List all available predefined paths"""
    return {
        'bronze': {
            'ejud_tab_xml': ejud_bronze_tab_xml(),
            'ejud_t_processo': ejud_bronze_t_processo(),
            'ejud_tipoprocesso': ejud_bronze_tipoprocesso(),
            'ejud_orgao': ejud_bronze_orgao(),
            'eproc2g_historico_distribuicao_judicial': eproc2g_historico_distribuicao_judicial(),
            'ejud_movimentorelator': ejud_bronze_movimentorelator(),
            'ejud_processoassunto': ejud_bronze_processoassunto(),
            'ejud_assunto': ejud_bronze_assunto(),
            'ejud_competencia': ejud_bronze_competencia(),
            'eproc_processo': eproc_bronze_processo(),
            'eproc_movimento': eproc_bronze_movimento(),
        },
        'silver': {
            'ejud_situacao': ejud_silver(),
            'ejud_initial': ejud_initial(),
            'ejud_filled': ejud_filled(),
            'eproc_situacao': eproc_silver(),
        },
        'gold': {
            'meta_01_base': meta_01_analytics_base(),
            'meta_01_ejud': meta_01_analytics_ejud(),
            'meta_01_eproc': meta_01_analytics_eproc(),
            'meta_02_base': meta_02_analytics_base(),
            'meta_02_ejud': meta_02_analytics_ejud(),
            'meta_02_eproc': meta_02_analytics_eproc(),
            'combined_meta': combined_meta_analytics(),
            'eproc2g_acervo': ouro_eproc2g_acervo(),
        }
    }
