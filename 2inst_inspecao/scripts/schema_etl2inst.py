import os
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, Text, Boolean,
    TIMESTAMP, CheckConstraint, UniqueConstraint, ForeignKey, func, event, DDL, insert, text
)
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from modulo.engine import create_postgres_engine

# ------------------------------------------------------------------
# Engine
# ------------------------------------------------------------------
load_dotenv("/opt/parquet/airflow_2inst/.env")

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
schema_name = "etl2inst"
metadata = MetaData(schema=schema_name)

# ------------------------------------------------------------------
# Drop and recreate schema
# ------------------------------------------------------------------
with engine.connect() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
    conn.execute(text(f"CREATE SCHEMA {schema_name}"))
    conn.commit()

# ------------------------------------------------------------------
# ETL Tasks Table
# ------------------------------------------------------------------
etl_tasks = Table(
    "etl_tasks",
    metadata,
    Column("task_id", Integer, primary_key=True, autoincrement=True),
    Column("task_name", Text, nullable=False, unique=True),
    Column("task_type", Text, nullable=False),  # extract_load, transform, production
    Column("connection_source", Text),
    Column("connection_target", Text),
    Column("source_schema", Text),
    Column("source_table", Text),
    Column("target_schema", Text, nullable=False),
    Column("target_table", Text, nullable=False),
    Column("load_type", Text),                 # full/incremental for extract/load
    Column("incremental_column", Text),
    Column("enabled", Boolean, default=False, nullable=False),
    Column("description", Text),
    Column("created_at", TIMESTAMP, server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=func.now(), nullable=False),
    CheckConstraint("load_type IN ('full','incremental')", name="chk_load_type")
)

# ------------------------------------------------------------------
# ETL Task Columns Table
# ------------------------------------------------------------------
# etl_task_columns = Table(
#     "etl_task_columns",
#     metadata,
#     Column("task_column_id", Integer, primary_key=True, autoincrement=True),
#     Column("task_id", Integer, ForeignKey("etl_tasks.task_id", ondelete="CASCADE"), nullable=False),
#     Column("column_name", Text, nullable=False),
#     Column("column_order", Integer),
#     Column("enabled", Boolean, default=False, nullable=False),
#     Column("merge_key", Boolean, default=False, nullable=True),
#     Column("created_at", TIMESTAMP, server_default=func.now(), nullable=False),
#     Column("updated_at", TIMESTAMP, server_default=func.now(), nullable=False),
#     UniqueConstraint("task_id", "column_name", name="uq_task_column")
# )

# ------------------------------------------------------------------
# ETL Task Dependencies Table
# ------------------------------------------------------------------
etl_task_dependencies = Table(
    "etl_task_dependencies",
    metadata,
    Column("dependency_id", Integer, primary_key=True, autoincrement=True),
    Column("task_id", Integer, ForeignKey("etl_tasks.task_id", ondelete="CASCADE"), nullable=False),
    Column("depends_on_task_id", Integer, ForeignKey("etl_tasks.task_id", ondelete="CASCADE"), nullable=False),
    Column("created_at", TIMESTAMP, server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=func.now(), nullable=False),
    UniqueConstraint("task_id", "depends_on_task_id", name="uq_task_dependency")
)

# ------------------------------------------------------------------
# Trigger for updated_at
# ------------------------------------------------------------------
update_trigger_function = DDL("""
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$ 
BEGIN 
    NEW.updated_at = NOW();
    RETURN NEW; 
END;
$$ LANGUAGE plpgsql;
""")

update_trigger = DDL(f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'trg_update_etl_tasks'
    ) THEN
        CREATE TRIGGER trg_update_etl_tasks
        BEFORE UPDATE ON {schema_name}.etl_tasks
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
    END IF;
END;
$$;
""")

event.listen(metadata, "before_create", update_trigger_function)
event.listen(metadata, "after_create", update_trigger)

# ------------------------------------------------------------------
# Create all tables
# ------------------------------------------------------------------
metadata.create_all(engine)

# ------------------------------------------------------------------
# Insert ETL Tasks (Extract/Load, Transform, Production)
# ------------------------------------------------------------------
etl_tasks_data = [

    # {"task_name": "", "task_type": "extract_load",
    # "connection_source": "oracle", "connection_target": "postgresql",
    # "source_schema": "proccorp", "source_table": "processo",
    # "target_schema": "proccorp", "target_table": "processo",
    # "load_type": "full", "incremental_column": "", "enabled": False},

    # Transform Tasks
    {"task_name": "transform_dim_processo_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "dim_processo_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_dim_processo_eproc2g", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "eproc2g", "source_table": None,
    "target_schema": "stg2inst", "target_table": "dim_processo_eproc2g",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_acervo_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_acervo_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_acervo_indicadores_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "stg2inst", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_acervo_indicadores_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_movimento_suspensao_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_movimento_suspensao_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_movimento_reativacao_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_movimento_reativacao_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_movimento_repercussao_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_movimento_repercussao_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_movimento_julgamento_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_movimento_julgamento_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_movimento_conclusao_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_movimento_conclusao_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_movimentofisico_acervo_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_movimentofisico_acervo_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_localvirtualdocumento_acervo_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_localvirtualdocumento_acervo_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_movimento_acervo_ejud", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "ejud", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_movimento_acervo_ejud",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_acervo_eproc2g", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "eproc2g", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_acervo_eproc2g",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_acervo_indicadores_eproc2g", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "stg2inst", "source_table": None,
    "target_schema": "stg2inst", "target_table": "tb_acervo_indicadores_eproc2g",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "transform_dim_magistrado_cpf", "task_type": "transform",
    "connection_source": None, "connection_target": None,
    "source_schema": "", "source_table": None,
    "target_schema": "insp2inst", "target_table": "dim_magistrado_cpf",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    # Production Tasks

    {"task_name": "production_acervo_inspecao", "task_type": "production",
    "connection_source": None, "connection_target": None,
    "source_schema": "", "source_table": None,
    "target_schema": "insp2inst", "target_table": "tb_acervo_inspecao",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    {"task_name": "production_dim_processo", "task_type": "production",
    "connection_source": None, "connection_target": None,
    "source_schema": "", "source_table": None,
    "target_schema": "insp2inst", "target_table": "dim_processo",
    "load_type": None, "incremental_column": None,
    "enabled": True},

    # Extract / Load Tasks

    # {"task_name": "load_ejud_tipoprocesso", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "tipoprocesso",
    #  "target_schema": "ejud", "target_table": "tipoprocesso",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_ejud_processoassunto", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "processoassunto",
    #  "target_schema": "ejud", "target_table": "processoassunto",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_eproc2g_processo", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "eproc2g", "source_table": "processo",
    #  "target_schema": "eproc2g", "target_table": "processo",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_eproc2g_processo_dado_complementar", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "eproc2g", "source_table": "processo_dado_complementar",
    #  "target_schema": "eproc2g", "target_table": "processo_dado_complementar",
    #  "load_type": "full", "incremental_column": "", "enabled": False},
    
    # {"task_name": "load_eproc2g_dado_complementar_valor", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "eproc2g", "source_table": "dado_complementar_valor",
    #  "target_schema": "eproc2g", "target_table": "dado_complementar_valor",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_eproc2g_dado_complementar", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "eproc2g", "source_table": "dado_complementar",
    #  "target_schema": "eproc2g", "target_table": "dado_complementar",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_eproc2g_classe_judicial", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "eproc2g", "source_table": "classe_judicial",
    #  "target_schema": "eproc2g", "target_table": "classe_judicial",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_eproc2g_competencia_judicial", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "eproc2g", "source_table": "competencia_judicial",
    #  "target_schema": "eproc2g", "target_table": "competencia_judicial",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_eproc2g_relacao_classe_judicial_mni", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "eproc2g", "source_table": "relacao_classe_judicial_mni",
    #  "target_schema": "eproc2g", "target_table": "relacao_classe_judicial_mni",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_ejud_orgaojulgador", "task_type": "extract_load",
    #  "connection_source": "mysql", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "orgaojulgador",
    #  "target_schema": "ejud", "target_table": "orgaojulgador",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_ejud_competencia", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "competencia",
    #  "target_schema": "ejud", "target_table": "competencia",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_ejud_movimento", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "movimento",
    #  "target_schema": "ejud", "target_table": "movimento",
    #  "load_type": "incremental", "incremental_column": "dthrmov", "enabled": True},

    # {"task_name": "load_ejud_movimentorelator", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "movimentorelator",
    #  "target_schema": "ejud", "target_table": "movimentorelator",
    #  "load_type": "incremental", "incremental_column": "dthrmov", "enabled": True},

    # {"task_name": "load_ejud_movdistribuicao", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "movdistribuicao",
    #  "target_schema": "ejud", "target_table": "movdistribuicao",
    #  "load_type": "full", "incremental_column": "", "enabled": True},

    # {"task_name": "load_ejud_movcancedistr", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "movcancedistr",
    #  "target_schema": "ejud", "target_table": "movcancedistr",
    #  "load_type": "full", "incremental_column": "", "enabled": True},

    # {"task_name": "load_ejud_movimentofisico", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "movimentofisico",
    #  "target_schema": "ejud", "target_table": "movimentofisico",
    #  "load_type": "full", "incremental_column": "", "enabled": True},

    #  {"task_name": "load_ejud_autoassociado", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "autoassociado",
    #  "target_schema": "ejud", "target_table": "autoassociado",
    #  "load_type": "full", "incremental_column": "", "enabled": False},

    # {"task_name": "load_ejud_t_processo", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "ejud", "source_table": "t_processo",
    #  "target_schema": "ejud", "target_table": "t_processo",
    #  "load_type": "full", "incremental_column": "coddoc", "enabled": False},

    #  {"task_name": "load_dcp_processo", "task_type": "extract_load",
    #  "connection_source": "oracle", "connection_target": "postgresql",
    #  "source_schema": "dcp", "source_table": "processo",
    #  "target_schema": "dcp", "target_table": "processo",
    #  "load_type": "full", "incremental_column": "data_dis", "enabled": False},

    #  {"task_name": "load_pje_tb_cabecalho_processo", "task_type": "extract_load",
    #  "connection_source": "pje_postgres", "connection_target": "postgresql",
    #  "source_schema": "client", "source_table": "tb_cabecalho_processo",
    #  "target_schema": "pje", "target_table": "tb_cabecalho_processo",
    #  "load_type": "full", "incremental_column": "dt_autuacao", "enabled": False},

    #  {"task_name": "load_pje_tb_classe_judicial", "task_type": "extract_load",
    #  "connection_source": "pje_postgres", "connection_target": "postgresql",
    #  "source_schema": "client", "source_table": "tb_classe_judicial",
    #  "target_schema": "pje", "target_table": "tb_classe_judicial",
    #  "load_type": "full", "incremental_column": "", "enabled": False},
     
]

with Session(engine) as session:
    # 1️⃣ Insert tasks and get task_id mapping
    result = session.execute(
        insert(etl_tasks).returning(etl_tasks.c.task_id, etl_tasks.c.task_name),
        etl_tasks_data
    )
    task_id = {row.task_name: row.task_id for row in result.fetchall()}
    session.commit()

    # 2️⃣ Build task columns now that we have task_id

    # 3️⃣ Build task dependencies now that we have task_id
    etl_task_dependencies_data = [
        
        # transform_ejud_dim_processo dependencies
        # {"task_id": task_id["transform_ejud_dim_processo"], "depends_on_task_id": task_id["load_ejud_t_processo"]},
        # {"task_id": task_id["transform_ejud_dim_processo"], "depends_on_task_id": task_id["load_ejud_tipoprocesso"]},
        # {"task_id": task_id["transform_ejud_dim_processo"], "depends_on_task_id": task_id["load_ejud_processoassunto"]},
        # {"task_id": task_id["transform_ejud_dim_processo"], "depends_on_task_id": task_id["load_ejud_competencia"]},
        # {"task_id": task_id["transform_eproc2g_dim_processo"], "depends_on_task_id": task_id["load_eproc2g_processo"]},
        # {"task_id": task_id["transform_eproc2g_dim_processo"], "depends_on_task_id": task_id["load_eproc2g_processo_dado_complementar"]},     
        # {"task_id": task_id["transform_eproc2g_dim_processo"], "depends_on_task_id": task_id["load_eproc2g_dado_complementar_valor"]},
        # {"task_id": task_id["transform_eproc2g_dim_processo"], "depends_on_task_id": task_id["load_eproc2g_dado_complementar"]},
        # {"task_id": task_id["transform_eproc2g_dim_processo"], "depends_on_task_id": task_id["load_eproc2g_classe_judicial"]},
        # {"task_id": task_id["transform_eproc2g_dim_processo"], "depends_on_task_id": task_id["load_eproc2g_competencia_judicial"]},
        # {"task_id": task_id["transform_eproc2g_dim_processo"], "depends_on_task_id": task_id["load_eproc2g_relacao_classe_judicial_mni"]},
        {"task_id": task_id["transform_localvirtualdocumento_acervo_ejud"], "depends_on_task_id": task_id["transform_acervo_ejud"]},
        {"task_id": task_id["transform_movimentofisico_acervo_ejud"], "depends_on_task_id": task_id["transform_acervo_ejud"]},
        {"task_id": task_id["transform_movimento_acervo_ejud"], "depends_on_task_id": task_id["transform_acervo_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_acervo_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_movimento_julgamento_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_movimento_suspensao_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_movimento_reativacao_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_movimento_repercussao_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_movimento_conclusao_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_movimentofisico_acervo_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_localvirtualdocumento_acervo_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_ejud"], "depends_on_task_id": task_id["transform_movimento_acervo_ejud"]},
        {"task_id": task_id["transform_acervo_indicadores_eproc2g"], "depends_on_task_id": task_id["transform_acervo_eproc2g"]},
        {"task_id": task_id["production_dim_processo"], "depends_on_task_id": task_id["transform_dim_processo_ejud"]},
        {"task_id": task_id["production_dim_processo"], "depends_on_task_id": task_id["transform_dim_processo_eproc2g"]},
        {"task_id": task_id["production_acervo_inspecao"], "depends_on_task_id": task_id["transform_acervo_indicadores_eproc2g"]},
        {"task_id": task_id["production_acervo_inspecao"], "depends_on_task_id": task_id["transform_acervo_indicadores_ejud"]},

    
      ]

    session.execute(insert(etl_task_dependencies), etl_task_dependencies_data)
    session.commit()
