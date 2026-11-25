import sys
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession  # noqa: E402

from eproc_metacnj.config import Meta01Rules  # noqa: E402
from eproc_metacnj.sources import SITUACAO_COLS  # noqa: E402

SIT_SCHEMA = (
    "id_processo STRING, dth_inicio TIMESTAMP, situacao INT, id_classe_judicial INT, "
    "id_assunto_principal INT, id_orgao_juizo INT, id_orgao_colegiado INT, id_sigilo INT, "
    "cod_mni_movimento STRING, num_processo STRING"
)
assert len(SIT_SCHEMA.split(",")) == len(SITUACAO_COLS)

PERIOD_START = date(2026, 1, 1)
PERIOD_END = date(2026, 12, 31)


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("eproc_metacnj_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="session")
def rules():
    return Meta01Rules(
        cod_sistema=2,
        cod_indicador={"cnm01": 1, "julgadom01": 2, "susm01": 3, "desm01": 4, "PendJulg01TG": 5},
        dist_start=date(1988, 1, 1),
        competencia_exclude=[2, 9],
        conhecimento_mni_codes=[7],
        trigger_codes=[24, 9, 65, 81],
        prim_sent_codes=[27],
        baixa_codes=[2],
        suspension_add_codes=[128],
        suspension_remove_codes=[138],
    )


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def sit_row(pid: str, when: str, situacao: int):
    return (pid, ts(when), situacao, 7, 100, 1, 50, None, None, f"num-{pid}")


@pytest.fixture
def make_situacao(spark):
    def _make(rows):
        return spark.createDataFrame([sit_row(*r) for r in rows], SIT_SCHEMA)
    return _make


@pytest.fixture
def make_ids(spark):
    def _make(ids):
        return spark.createDataFrame([(i,) for i in ids], "id_processo STRING")
    return _make
