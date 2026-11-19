#!/usr/bin/env python3
"""
Parquet Path Utilities for CNJ 2026 Meta Indicators

Provides environment-driven configuration for parquet locations.
Supports both local filesystem and S3 storage based on AMBIENTE setting.

Environment Variables:
- PARQUET_HOME: Base directory for local parquet storage (default: /opt/parquet)
- AMBIENTE: Environment type - 'local' or 's3' (default: local)
- S3A_BUCKET: S3 bucket name for cloud storage (default: sgdai-data-lake)
"""

import os
from pathlib import Path
from typing import Optional

# Environment-driven configuration
PARQUET_HOME = os.environ.get('PARQUET_HOME', '/opt/parquet')
AMBIENTE = os.environ.get('AMBIENTE', 'local').lower()
S3A_BUCKET = os.environ.get('S3A_BUCKET', 'sgdai-data-lake')


def parquet_path(rel_path: str) -> str:
    """Return a full parquet path for the current environment.

    rel_path should be a POSIX-style path relative to the PARQUET_HOME root
    e.g. 'data/prata/ejud/situacao_ejud' or 'datahj[]
    meta_02'.

    If AMBIENTE == 'local' the returned path is under PARQUET_HOME. Otherwise
    it returns an s3a:// path using S3A_BUCKET.
    """
    # Normalize rel_path and strip leading slashes
    p = rel_path.lstrip('/\\')
    if AMBIENTE == 'local':
        return str(Path(PARQUET_HOME) / p)
    else:
        return f"s3a://{S3A_BUCKET}/{p}"


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


# Convenience helpers for common CNJ 2026 paths
def ejud_situacao_base():
    """Base path for EJUD situacao data."""
    return parquet_path('data/prata/ejud/situacao_ejud')


def eproc_situacao_base():
    """Base path for EPROC situacao data."""
    return parquet_path('data/prata/eproc2g/situacao_eproc')


def ejud_export_base():
    """Base path for EJUD meta exports."""
    return parquet_path('data/prata/ejud/meta_cnj2026')

def ejud_bronze_base():
    """Base path for EJUD bronze data."""
    return parquet_path('data/bronze/ejud/')

def eproc_export_base():
    """Base path for EPROC meta exports."""
    return parquet_path('data/prata/eproc2g/meta_cnj2026')


def spark_events_path():
    """Path for Spark event logs."""
    return parquet_path('sgdai/dev/dev-felipe/meta_cnj2026/spark-events')


def meta_export_path(meta_name: str, system: str = "ejud"):
    """Path for meta export results."""
    if system.lower() == "ejud":
        return parquet_path(f'data/prata/ejud/meta_cnj2026/{meta_name}/current')
    elif system.lower() == "eproc":
        return parquet_path(f'data/prata/eproc2g/meta_cnj2026/{meta_name}/current')
    elif system.lower() == "eproc2g":
        return parquet_path(f'data/prata/eproc2g/meta_cnj2026/{meta_name}/current')
    else:
        raise ValueError(f"Unknown system: {system}")


def documento_path(system: str = "ejud"):
    """Path for documento data."""
    if system.lower() == "ejud":
        return parquet_path('data/bronze/ejud/documento/')
    elif system.lower() == "eproc":
        return parquet_path('data/bronze/eproc2g/documento/')
    elif system.lower() == "eproc2g":
        return parquet_path('data/bronze/eproc2g/documento/')
    else:
        raise ValueError(f"Unknown system: {system}")

def movimento_path(system: str = "ejud"):
    """Path for movimento data."""
    if system.lower() == "ejud":
        return parquet_path('data/bronze/ejud/movimento/')
    else:
        raise ValueError(f"Unknown system: {system}")


def filtered_coddoc_path():
    """Path for filtered coddoc files."""
    return parquet_path('data/prata/filtered_coddoc_classe_conhecimento.parquet')


# Environment info for logging/debugging
def get_environment_info() -> dict:
    """Get current environment configuration info."""
    return {
        'ambiente': AMBIENTE,
        'parquet_home': PARQUET_HOME,
        's3a_bucket': S3A_BUCKET,
        'is_s3': AMBIENTE != 'local'
    }


def format_situacao(situacao, width: int = 3) -> str:
    """Return a zero-padded situacao string safely.

    Accepts int or str inputs. If input cannot be coerced to int, returns
    the original string left-padded or as-is.
    """
    try:
        return str(int(situacao)).zfill(width)
    except Exception:
        try:
            # Fallback: if it's already a string of digits, zfill it
            s = str(situacao)
            if s.isdigit():
                return s.zfill(width)
            return s
        except Exception:
            return str(situacao)


def tipoprocesso_path(system: str) -> str:
    """Return the path to tipoprocesso parquet files.
    
    Args:
        system: Either "ejud" or "eproc"
        
    Returns:
        Absolute path to tipoprocesso parquet files
    """
    if system.lower() == "ejud":
        return parquet_path("data/bronze/ejud/tipoprocesso")
    elif system.lower() == "eproc":
        return parquet_path("data/bronze/eproc/tipoprocesso")
    else:
        raise ValueError(f"Unsupported system: {system}. Must be 'ejud' or 'eproc'")


def processo_path(system: str) -> str:
    """Return the path to processo (t_processo) parquet files.
    
    Args:
        system: Either "ejud" or "eproc"
        
    Returns:
        Absolute path to processo parquet files
    """
    if system.lower() == "ejud":
        return parquet_path("data/bronze/ejud/t_processo")
    elif system.lower() == "eproc":
        return parquet_path("data/bronze/eproc/processo")
    else:
        raise ValueError(f"Unsupported system: {system}. Must be 'ejud' or 'eproc'")
