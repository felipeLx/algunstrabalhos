"""Settings (tables/locations) and Meta 01 rules (situacao codes) for eproc2g."""

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

import boto3

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = (
    "s3://sgdai-projetos/2instancia/2inst_metacnj/eproc2g/json/meta_config.json"
)

# Production defaults. The target is fixed in ouro on purpose: a stale deployed
# meta_config.json must not redirect production writes. Only CLI flags override it.
CATALOG = "glue_catalog"
WAREHOUSE = "s3://sgdai-data-lake"
TARGET_DB = "dw_2inst_ouro"
TARGET_TABLE = "tb_ft_meta_analitico"
DB_SITUACAO = "dw_2inst_ouro"
SITUACAO_TABLE = "tb_tf_situacao_eproc"
DB_BRONZE = "eproc2g"


def default_target_loc(target_db: str = TARGET_DB) -> str:
    return f"{WAREHOUSE}/ouro/{target_db}/{TARGET_TABLE}"


def load_config(path: str) -> dict:
    """Read meta_config.json from a local path or s3://bucket/key."""
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@dataclass(frozen=True)
class Settings:
    """Where data is read from and written to."""

    catalog: str = CATALOG
    warehouse: str = WAREHOUSE
    db_situacao: str = DB_SITUACAO
    situacao_table: str = SITUACAO_TABLE
    db_bronze: str = DB_BRONZE
    target_db: str = TARGET_DB
    target_table: str = TARGET_TABLE
    target_loc: str = default_target_loc()

    def fqn(self, db: str, table: str) -> str:
        return f"{self.catalog}.{db}.{table}"

    @property
    def situacao_fqn(self) -> str:
        return self.fqn(self.db_situacao, self.situacao_table)

    @property
    def target_fqn(self) -> str:
        return self.fqn(self.target_db, self.target_table)

    def bronze(self, table: str) -> str:
        return self.fqn(self.db_bronze, table)


def resolve_settings(
    cfg: dict,
    catalog: str = CATALOG,
    db_situacao: str = DB_SITUACAO,
    db_bronze: str = DB_BRONZE,
    target_db: Optional[str] = None,
    target_loc: Optional[str] = None,
) -> Settings:
    """Build Settings from CLI values. Config target is only compared, never used."""
    cat_cfg = cfg.get("catalog", {})
    if (
        cat_cfg.get("target_db") != TARGET_DB
        or cat_cfg.get("target_table") != TARGET_TABLE
        or cat_cfg.get("target_loc") != default_target_loc()
    ):
        logger.warning(
            "Config target differs from production default; using %s.%s at %s "
            "unless CLI override is provided.",
            TARGET_DB, TARGET_TABLE, default_target_loc(),
        )

    return Settings(
        catalog=catalog,
        db_situacao=db_situacao,
        db_bronze=db_bronze,
        target_db=target_db or TARGET_DB,
        target_loc=target_loc or default_target_loc(),
    )


@dataclass(frozen=True)
class Meta01Rules:
    """Situacao codes and constants that define the Meta 01 indicators."""

    cod_sistema: int                      # 2 = eproc2g
    cod_indicador: Dict[str, int]         # variavel -> cod_indicador
    dist_start: date                      # earliest date scanned for history
    competencia_exclude: List[int]
    conhecimento_mni_codes: List[int]     # CNJ class codes counted as "conhecimento"
    trigger_codes: List[int]              # distribution codes (eproc2g: no sit-61)
    prim_sent_codes: List[int]            # first judgment
    baixa_codes: List[int]                # case closed
    suspension_add_codes: List[int]
    suspension_remove_codes: List[int]

    PENDENTE = 88                         # sit-88 marks the case as pending (new case)

    @classmethod
    def from_config(cls, cfg: dict) -> "Meta01Rules":
        shared = cfg["shared"]
        codes = shared["situacao_codes"]
        m1 = cfg["meta_01"]
        return cls(
            cod_sistema=cfg["catalog"]["ind_sistema"],
            cod_indicador=m1["cod_indicador_map"],
            dist_start=date.fromisoformat(shared["dist_start"]),
            competencia_exclude=shared["competencia_exclude"],
            conhecimento_mni_codes=m1["conhecimento_mni_codes"],
            trigger_codes=m1["conhecimento_dist_codes"] + m1["outros_dist_codes"],
            prim_sent_codes=codes["prim_sent"],
            baixa_codes=codes["baixa"],
            suspension_add_codes=codes["suspension_add"],
            suspension_remove_codes=codes["suspension_remove"],
        )
