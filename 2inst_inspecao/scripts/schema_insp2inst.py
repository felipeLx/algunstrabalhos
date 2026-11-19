import os
from sqlalchemy import (create_engine, MetaData, Table, Column, Date, String, BigInteger, Integer, Numeric, CHAR, DateTime, ForeignKey, PrimaryKeyConstraint, ForeignKeyConstraint)
from dotenv import load_dotenv
from sqlalchemy import text
from modulo.engine import create_postgres_engine

# ------------------------------------------------------------------
# Engine (adjust connection string to your Postgresql environment)
# ------------------------------------------------------------------

load_dotenv("/opt/parquet/airflow_2inst/.env")

POSTGRESQL_SL_HOST = os.getenv("POSTGRESQL_SL_HOST")
POSTGRESQL_SL_DBNAME = os.getenv("POSTGRESQL_SL_DBNAME")
POSTGRESQL_SL_PORT = os.getenv("POSTGRESQL_SL_PORT")
POSTGRESQL_SL_USER = os.getenv("POSTGRESQL_SL_USER")
POSTGRESQL_SL_PASSWORD = os.getenv("POSTGRESQL_SL_PASSWORD")

engine = create_postgres_engine(
    host=os.getenv("POSTGRESQL_SL_HOST"),
    dbname=os.getenv("POSTGRESQL_SL_DBNAME"),
    port=os.getenv("POSTGRESQL_SL_PORT"),
    user=os.getenv("POSTGRESQL_SL_USER"),
    password=os.getenv("POSTGRESQL_SL_PASSWORD"),
    echo=True
)

# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------
schema_name = "insp2inst"
metadata = MetaData(schema=schema_name)

# ------------------------------------------------------------------
# Drop and recreate schema
# ------------------------------------------------------------------
with engine.connect() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
    conn.execute(text(f"CREATE SCHEMA {schema_name}"))
    conn.commit()

# ------------------------------------------------------------------
# DIMENSOES
# ------------------------------------------------------------------

dim_magistrado_cpf = Table(
    "dim_magistrado_cpf",
    metadata,
    Column("id_cpf", String(20), primary_key=True),
    Column("nm_magistrado", String(100)),
    Column("ind_ejud", String(10)),
    Column("ind_eproc2g", String(10)),
)

dim_orgao = Table(
    "dim_orgao",
    metadata,
    Column("cod_orgao", Integer, primary_key=True),
    Column("cod_orgao_cnj", Integer),
    Column("nm_orgao_scol", String(100)),
    Column("nm_orgao", String(100)),
    Column("cod_competencia", Integer),
    Column("nm_competencia", String(100)),
    Column("nm_sigla", String(20)),
    Column("ind_ejud", String(10)),
    Column("ind_eproc2g", String(10)),
    Column("ind_distribuicao", String(10)),
    Column("num_ordinal", Integer),
)

dim_processo = Table(
    "dim_processo",
    metadata,
    Column("cod_documento", Numeric(30, 0)),                                                # NUMERIC(30,0)
    Column("cod_cnj", String(30), nullable=False),                                          # VARCHAR(30)
    Column("cod_cnj_sigilo", String(30), nullable=False),                                   # VARCHAR(30)
    Column("id_sigilo", Integer, nullable=False),                                           # INTEGER
    Column("cod_classe_interna", Integer, nullable=False),                                  # INTEGER
    Column("cod_classe_externa", Integer, nullable=False),                                  # INTEGER
    Column("cod_classe_cnj", Integer),                                                      # INTEGER
    Column("nm_classe", String(100), nullable=False),                                       # VARCHAR(100)
    Column("ind_originario", CHAR(1), nullable=False),                                      # CHAR(1)
    Column("ind_prioridade_idoso", CHAR(1), nullable=False),                                # CHAR(1)
    Column("ind_prioridade_idoso_80", CHAR(1), nullable=False),                             # CHAR(1)
    Column("ind_doenca_grave", CHAR(1), nullable=False),                                    # CHAR(1)
    Column("ind_prioridade_deficiente", CHAR(1), nullable=False),                           # CHAR(1)
    Column("dth_autuacao", DateTime, nullable=False),
    Column("ind_crime_adm_publico", CHAR(1), nullable=False),
    Column("ind_menor", CHAR(1), nullable=False),
    Column("ind_menor_recolhido", CHAR(1), nullable=False),
    Column("ind_sistema", String(7), nullable=False),     
    PrimaryKeyConstraint("cod_documento", "ind_sistema", name="pk_dim_processo")            # STRING(5) → VARCHAR(5)
)

dim_movimento = Table(
    "dim_classe",
    metadata,
    Column("cod_movimento", Integer, primary_key=True),
    Column("nm_movimento", String(100)),
    Column("ind_sistema", Integer),
)

dim_local = Table(
    "dim_local",
    metadata,
    Column("cod_local", Integer, primary_key=True),
    Column("nm_local", String(100)),
    Column("ind_ejud", String(10)),
    Column("ind_eproc2g", String(10)),
)

dim_localvirtual = Table(
    "dim_localvirtual",
    metadata,
    Column("cod_local_virtual", Integer, primary_key=True),
    Column("nm_local_virtual", String(100)),
    Column("ind_sistema", Integer),
)

dim_localizador = Table(
    "dim_localizador",
    metadata,
    Column("cod_localizador", Integer, primary_key=True),
    Column("nm_localizador", String(100)),
    Column("ind_sistema", Integer),
)

# ------------------------------------------------------------------
# RELACIONAMENTOS
# ------------------------------------------------------------------

rel_associacao_magistrado = Table(
    "rel_associacao_magistrado",
    metadata,
    Column("cod_magistrado", Integer, nullable=False),
    Column("nm_magistrado_sistema", String(100)),
    Column("id_cpf", String(20), ForeignKey("dim_magistrado_cpf.id_cpf",name="fk_magistradocpf_insp"), nullable=False),
    Column("ind_sistema", String(10), nullable=False),
    PrimaryKeyConstraint("cod_magistrado", "ind_sistema", name="pk_associacao_magistrado")
)

rel_associacao_orgao = Table(
    "rel_associacao_orgao",
    metadata,
    Column("cod_orgao_antigo", Integer, nullable=False),
    Column("cod_orgao_novo", Integer, nullable=False),
    Column("ind_ejud", String(1)),
    Column("ind_eproc2g", String(1)),
    PrimaryKeyConstraint("cod_orgao_antigo", name="pk_associacao_orgao"),
    ForeignKeyConstraint(["cod_orgao_antigo"],["dim_orgao.cod_orgao"], name="fk_orgnovo_insp"),
    ForeignKeyConstraint(["cod_orgao_novo"],["dim_orgao.cod_orgao"], name="fk_organtigo_insp"),
)

rel_associacao_local = Table(
    "rel_associacao_local",
    metadata,
    Column("cod_local_sistema", Integer, nullable=False),
    Column("cod_local_parametrizado", Integer, nullable=False),
    Column("local_associado", String(10)),
    Column("ind_movimento_fisico", String(10)),
    Column("ind_sistema", String(10)),
    PrimaryKeyConstraint("cod_local_sistema","ind_sistema",name="pk_associacao_local"),
    ForeignKeyConstraint(["cod_local_parametrizado"],["dim_local.cod_local"], name="fk_local_insp"),
)

# ------------------------------------------------------------------
# FATOS
# ------------------------------------------------------------------

tb_movimento = Table(
    "tb_movimento",
    metadata,
    Column("dth_referencia", Date, nullable=False),
    Column("ind_referencia", String(15)),
    Column("cod_item", String(9)),
    Column("cod_documento", Integer, nullable=False),
    Column("cod_magistrado",Integer, nullable=False),
    Column("dth_ultimo_local", Date),
    Column("cod_ultimo_local", Integer),
    Column("cod_classe", Integer),
    Column("dth_movimento", Date, nullable=False),
    Column("cod_movimento", Integer, nullable=False),
    Column("ind_sistema", String(5)),
    Column("dth_insert", Date, nullable=False),
    Column("dth_update", Date, nullable=False),
    Column("dth_delete", Date, nullable=False),
    
    PrimaryKeyConstraint(
        "dth_referencia",
        "cod_documento",
        "cod_movimento",
        "dth_movimento",
        name="pk_mov_inspecaocnj"
    ),

    ForeignKeyConstraint(
        ["cod_documento", "ind_sistema"],
        ["dim_processo.cod_documento", "dim_processo.ind_sistema"],
        name="fk_dim_processo"
    ),

    ForeignKeyConstraint(
        ["cod_magistrado", "ind_sistema"],
        ["rel_associacao_magistrado.cod_magistrado",
         "rel_associacao_magistrado.ind_sistema"],
        name="fk_mov_assoc_magistrado"
    ),

    ForeignKeyConstraint(
        ["cod_ultimo_local", "ind_sistema"],
        ["rel_associacao_local.cod_local_sistema",
         "rel_associacao_local.ind_sistema"],
        name="fk_assoc_local_insp"
    ),
)

tb_acervo_inspecao= Table(
    "tb_acervo_inspecao",
    metadata,
    Column("cod_documento", Integer, nullable=False),
    Column("dth_referencia", Date, nullable=False),
    Column("ind_referencia", String(15)),
    Column("dth_ultima_distribuicao", Date),
    Column("cod_ultimo_relator",Integer),
    Column("cod_ultimo_orgao_julgador",Integer),
    Column("dth_ultimo_local", Date),
    Column("cod_ultimo_local",Integer),
    Column("dth_ultimo_local_virtual", Date, nullable=False),
    Column("cod_ultimo_local_virtual",Integer,ForeignKey("dim_localvirtual.cod_local_virtual",name="fk_codlocalvirtual_insp")),
    Column("dth_ultimo_localizador", Date, nullable=False),
    Column("cod_ultimo_localizador",Integer,ForeignKey("dim_localizador.cod_localizador",name="fk_codlocalizador_insp")),
    Column("dth_ultimo_movimento", Date, nullable=False),
    Column("cod_ultimo_movimento", Integer, nullable=False),
    Column("dth_suspensao", Date, nullable=False),
    Column("ind_suspensao", String(5)),
    Column("ind_sobrestado", String(5)),
    Column("dth_julgamento", Date, nullable=False),
    Column("ind_julgamento", String(5)),
    Column("dth_julgamento_monocratico", Date, nullable=False),
    Column("ind_julgamento_monocratico", String(5)),
    Column("dth_conclusao", Date, nullable=False),
    Column("ind_conclusao", String(5)),
    Column("ind_sistema", String(7)),
    Column("dth_insert", Date, nullable=False),
    Column("dth_update", Date, nullable=False),
    Column("dth_delete", Date, nullable=False),
    
    PrimaryKeyConstraint(
        "dth_referencia",
        "cod_documento",
        "ind_sistema",
        name="pk_acervo_inspecaocnj"
    ),

    ForeignKeyConstraint(
        ["cod_documento", "ind_sistema"],
        ["dim_processo.cod_documento", "dim_processo.ind_sistema"],
        name="fk_dim_processo_acervo"
    ),

    ForeignKeyConstraint(
        ["cod_ultimo_relator", "ind_sistema"],
        ["rel_associacao_magistrado.cod_magistrado",
         "rel_associacao_magistrado.ind_sistema"],
        name="fk_assoc_magistrado_insp"
    ),

    ForeignKeyConstraint(
        ["cod_ultimo_orgao_julgador"],
        ["rel_associacao_orgao.cod_orgao_antigo"],
        name="fk_assoc_orgaojulgador_insp"
    ),

    ForeignKeyConstraint(
        ["cod_ultimo_local", "ind_sistema"],
        ["rel_associacao_local.cod_local_sistema",
         "rel_associacao_local.ind_sistema"],
        name="fk_assoc_local_insp"
    ),
)

# ------------------------------------------------------------------
# Create tables in the database
# ------------------------------------------------------------------

metadata.create_all(engine)
