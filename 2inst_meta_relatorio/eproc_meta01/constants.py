"""Fixed values for eproc2g Meta 01. Change the year here, nowhere else."""

META_ANO = 2026
COD_SISTEMA = 2  # eproc2g

# variavel -> cod_indicador. PendJulg01TG is derived (cnm01 minus julgadom01), not read.
COD_INDICADOR = {
    "cnm01":        1,
    "julgadom01":   2,
    "susm01":       3,
    "desm01":       4,
    "PendJulg01TG": 5,
}

# Prata folder with one parquet per variable (cnm01.parquet, julgadom01.parquet, ...).
PRATA_DIR = f"data/prata/eproc2g/meta_cnj{META_ANO}/meta_01/current"

# Which variable files to read comes from config/variables_config.json -> meta_01.variables.case1.
DEFAULT_VARIABLES_CONFIG = "./config/variables_config.json"

# Postgres targets
PG_SCHEMA = "dw2inst"
META_ANALITICO_TABLE = "meta_analitico"
FATO_AUXILIAR_TABLE = "fato_auxiliar"
