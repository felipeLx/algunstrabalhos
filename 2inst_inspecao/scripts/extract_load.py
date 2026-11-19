import os
import sys
import logging
from dotenv import load_dotenv
from sqlalchemy import text

from modulo.engine import create_postgres_engine
from modulo.conexoes import (
    get_oracle_connection,
    get_postgres_connection,
    get_mysql_connection,
)
from modulo.utils import (
    oracle_to_postgres_full,
    oracle_to_postgres_incremental,
    mysql_to_postgres_full,
    mysql_to_postgres_incremental,
    postgres_to_postgres_full,
    postgres_to_postgres_incremental
)

# -------------------------------------------------
# Logging (Airflow-compatible)
# -------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------------------------------------
# Environment
# -------------------------------------------------
load_dotenv("/opt/parquet/projetos/.env")

META_SCHEMA = "etl2inst"
TZ = "America/Sao_Paulo"


def run_extract_load(task_config: dict, **context):
    """
    Metadata-driven extract/load task.
    """

    task_id = task_config["task_id"]
    task_name = task_config["task_name"]

    logging.info("[%s] START extract/load", task_name)

    # -------------------------------------------------
    # Metadata engine (Postgres)
    # -------------------------------------------------
    meta_engine = create_postgres_engine(
        host=os.getenv("POSTGRESQL_SL_HOST"),
        dbname=os.getenv("POSTGRESQL_SL_DBNAME"),
        port=os.getenv("POSTGRESQL_SL_PORT"),
        user=os.getenv("POSTGRESQL_SL_USER"),
        password=os.getenv("POSTGRESQL_SL_PASSWORD"),
        echo=False,
    )

    # -------------------------------------------------
    # Load enabled columns for task
    # -------------------------------------------------
    with meta_engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT column_name
                FROM {META_SCHEMA}.etl_task_columns
                WHERE task_id = :task_id
                  AND enabled = true
                ORDER BY column_order
            """),
            {"task_id": task_id},
        ).fetchall()

    if not rows:
        raise ValueError(f"No enabled columns found for task {task_name}")

    columns = [r[0] for r in rows]

    # -------------------------------------------------
    # Task metadata
    # -------------------------------------------------
    conn_source = task_config["connection_source"].lower()
    conn_target = task_config["connection_target"].lower()

    if conn_target != "postgresql":
        raise ValueError(f"Unsupported target connection: {conn_target}")

    source_schema = task_config["source_schema"]
    source_table = task_config["source_table"]
    target_schema = task_config["target_schema"]
    target_table = task_config["target_table"]

    load_type = (task_config.get("load_type") or "full").lower()
    incremental_col = task_config.get("incremental_column")

    # -------------------------------------------------
    # Airflow logical date
    # -------------------------------------------------
    logical_date = context["logical_date"].start_of("hour")

    if logical_date is None:
        raise ValueError("logical_date not found in Airflow context")

    logical_date = logical_date.in_timezone(TZ)
    logical_date_str = logical_date.format("YYYY-MM-DD HH:mm:ss")

    # -------------------------------------------------
    # Logging summary
    # -------------------------------------------------
    logging.info("=" * 70)
    logging.info("Task              : %s", task_name)
    logging.info("Source             : %s.%s", source_schema, source_table)
    logging.info("Target             : %s.%s", target_schema, target_table)
    logging.info("Source connection  : %s", conn_source)
    logging.info("Load type          : %s", load_type)
    logging.info("Columns            : %s", columns)
    if incremental_col:
        logging.info("Incremental column : %s", incremental_col)
    logging.info("Logical date       : %s", logical_date_str)
    logging.info("=" * 70)

    # -------------------------------------------------
    # Target Postgres connection
    # -------------------------------------------------
    pg_conn = get_postgres_connection(
        os.getenv("POSTGRESQL_SL_HOST"),
        os.getenv("POSTGRESQL_SL_PORT", "5432"),
        os.getenv("POSTGRESQL_SL_DBNAME"),
        os.getenv("POSTGRESQL_SL_USER"),
        os.getenv("POSTGRESQL_SL_PASSWORD"),
    )
    

    try:
        # -------------------------------------------------
        # Incremental state
        # -------------------------------------------------
        last_value = None
        target_fq = f"{target_schema}.{target_table}"

        if load_type == "incremental" and incremental_col:
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT MAX({incremental_col})
                    FROM {target_fq}
                    WHERE {incremental_col} <= %s
                    """,
                    (logical_date_str,),
                )
                last_value = cur.fetchone()[0]

            logging.info("Last incremental value: %s", last_value)

        # -------------------------------------------------
        # Build source SQL
        # -------------------------------------------------
        base_sql = f"""
            SELECT {', '.join(columns)}
            FROM {source_schema}.{source_table}
        """

        if load_type == "incremental" and last_value:
            if conn_source == "oracle":
                base_sql += f"""
                    WHERE {incremental_col} > TO_DATE('{last_value}', 'YYYY-MM-DD HH24:MI:SS')
                      AND {incremental_col} <= TO_DATE('{logical_date_str}', 'YYYY-MM-DD HH24:MI:SS')
                """
            elif conn_source == "mysql":
                base_sql += f"""
                    WHERE {incremental_col} > '{last_value}'
                      AND {incremental_col} <= '{logical_date_str}'
                """

        logging.info("Source SQL:\n%s", base_sql)

        # -------------------------------------------------
        # Execute ETL
        # -------------------------------------------------
        if conn_source == "oracle":
            ora = get_oracle_connection(
                os.getenv("ORACLE_TJ01_DB_USER"),
                os.getenv("ORACLE_TJ01_DB_PASSWORD"),
                os.getenv("ORACLE_TJ01_DB_DSN"),
            )
            try:
                fn = (
                    oracle_to_postgres_incremental
                    if load_type == "incremental" and incremental_col
                    else oracle_to_postgres_full
                )
                fn(
                    oracle_conn=ora,
                    postgres_conn=pg_conn,
                    source_schema=source_schema,
                    target_schema=target_schema,
                    table_name=source_table,
                    oracle_query=base_sql,
                    batch_size=100_000,
                    debug=True,
                )
            finally:
                ora.close()

        elif conn_source == "mysql":
            mysql = get_mysql_connection(
                os.getenv("MYSQL_DB2_HOST"),
                os.getenv("MYSQL_DB2_PORT", "3306"),
                os.getenv("MYSQL_DB2_USER"),
                os.getenv("MYSQL_DB2_PASSWORD"),
            )
            try:
                fn = (
                    mysql_to_postgres_incremental
                    if load_type == "incremental" and incremental_col
                    else mysql_to_postgres_full
                )
                fn(
                    mysql_conn=mysql,
                    postgres_conn=pg_conn,
                    schema_name=target_schema,
                    table_name=target_table,
                    mysql_query=base_sql,
                    batch_size=100_000,
                    debug=True,
                )
            finally:
                mysql.close()

        ##PJE Source connection
        elif conn_source == "pje_postgres":
            source_pg = get_postgres_connection(
                os.getenv("POSTGRESQL_DB_HOST"),
                os.getenv("POSTGRESQL_DB_PORT", "5432"),
                os.getenv("POSTGRESQL_DB_DBNAME"),
                os.getenv("POSTGRESQL_DB_USER"),
                os.getenv("POSTGRESQL_DB_PASSWORD"),
            )

            try:
                fn = (
                    postgres_to_postgres_incremental
                    if load_type == "incremental" and incremental_col
                    else postgres_to_postgres_full
                )

                fn(
                    source_conn=source_pg,
                    target_conn=pg_conn,
                    source_schema=source_schema,
                    target_schema=target_schema,
                    table_name=target_table,
                    source_query=base_sql,
                    batch_size=100_000,
                    debug=True,
                )

            finally:
                source_pg.close()

        else:
            raise ValueError(f"Unsupported source connection: {conn_source}")

        logging.info("[SUCCESS] %s completed", task_name)

    except Exception:
        logging.exception("[FAILED] %s", task_name)
        raise

    finally:
        pg_conn.close()
        logging.info("[END] %s", task_name)
