import os
import io
import time
import logging
import sys
from dotenv import load_dotenv

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
)

# -----------------------------
# Logging (Airflow-compatible)
# -----------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------
# Load environment
# -----------------------------
load_dotenv("/opt/parquet/projetos/.env")


def run_extract_load(table_config: dict, **context):
    """
    Run ETL for a single table (full or incremental).
    Incremental uses the target Postgres table to determine the last value.
    """

    task_id = table_config["task_id"].lower()
    logging.info("[%s] Starting load", task_id)

    # -----------------------------
    # Required config keys
    # -----------------------------
    required_keys = {"connection", "source_schema", "target_schema", "table_name", "columns"}
    missing = required_keys - table_config.keys()
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")

    # -----------------------------
    # Extract config
    # -----------------------------
    conn_type = table_config["connection"].lower()
    source_schema = table_config["source_schema"]
    target_schema = table_config["target_schema"]
    table_name = table_config["table_name"]
    columns = table_config["columns"]
    incremental_col = table_config.get("incremental_column")
    load_type = table_config.get("load_type", "full").lower()

    # -----------------------------
    # Logical date from Airflow
    # -----------------------------
    logical_date = context.get("logical_date") or context.get("execution_date")
    if logical_date is None:
        raise ValueError("logical_date not available in Airflow context")
    logical_date = logical_date.in_timezone("America/Sao_Paulo").start_of("hour")
    logical_date_str = logical_date.format("YYYY-MM-DD HH:mm:ss")

    # -----------------------------
    # Basic Logging
    # -----------------------------
    logging.info("=" * 60)
    logging.info("[LOAD START] %s.%s", source_schema, table_name)
    logging.info("Source type       : %s", conn_type)
    logging.info("Columns           : %s", columns)
    logging.info("Load type         : %s", load_type)
    if incremental_col:
        logging.info("Incremental column: %s", incremental_col)
    logging.info("Logical date      : %s", logical_date_str)
    logging.info("=" * 60)

    # -----------------------------
    # Connect to Postgres (target)
    # -----------------------------
    pg_conn = get_postgres_connection(
        os.getenv("POSTGRESQL_SL_HOST"),
        os.getenv("POSTGRESQL_SL_PORT", "5432"),
        os.getenv("POSTGRESQL_SL_DBNAME"),
        os.getenv("POSTGRESQL_SL_USER"),
        os.getenv("POSTGRESQL_SL_PASSWORD"),
    )

    try:
        # -----------------------------
        # Determine last incremental value
        # -----------------------------
        last_value = None
        target_table = f"{target_schema}.{source_schema}_{table_name}"  # table in postgresql

        if incremental_col and load_type == "incremental":
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT MAX({incremental_col})
                    FROM {target_table}
                    WHERE {incremental_col} <= %s
                    """,
                    (logical_date_str,)
                )
                row = cur.fetchone()
                last_value = row[0] if row and row[0] is not None else None

            logging.info(
                "[LOAD DEBUG] Last incremental value in %s: %s",
                target_table,
                last_value,
            )

        # -----------------------------
        # Build source query
        # -----------------------------
        source_table = f"{source_schema}.{table_name}"
        base_sql = f"SELECT {', '.join(columns)} FROM {source_table}"
        if incremental_col and last_value is not None and load_type == "incremental":
            base_sql += (
                f" WHERE {incremental_col} > TO_DATE('{last_value}', 'YYYY-MM-DD HH24:MI:SS')"
                f" AND {incremental_col} <= TO_DATE('{logical_date_str}', 'YYYY-MM-DD HH24:MI:SS')"
            )
            logging.info("[LOAD DEBUG] Incremental filter applied: %s > %s AND %s <= %s",
                         incremental_col, last_value, incremental_col, logical_date_str)

        logging.info("[LOAD DEBUG] SQL to execute:\n%s", base_sql)

        # -----------------------------
        # Execute ETL
        # -----------------------------
        if conn_type == "oracle":
            ora_conn = get_oracle_connection(
                os.getenv("ORACLE_TJ01_DB_USER"),
                os.getenv("ORACLE_TJ01_DB_PASSWORD"),
                os.getenv("ORACLE_TJ01_DB_DSN"),
            )
            try:
                if load_type == "incremental" and incremental_col:
                    oracle_to_postgres_incremental(
                        oracle_conn=ora_conn,
                        postgres_conn=pg_conn,
                        source_schema=source_schema,
                        target_schema=target_schema,
                        table_name=table_name,
                        oracle_query=base_sql,
                        batch_size=1_000_000,
                        debug=True,
                    )
                else:
                    oracle_to_postgres_full(
                        oracle_conn=ora_conn,
                        postgres_conn=pg_conn,
                        source_schema=source_schema,
                        target_schema=target_schema,
                        table_name=table_name,
                        oracle_query=base_sql,
                        batch_size=1_000_000,
                        debug=True,
                    )
            finally:
                ora_conn.close()

        elif conn_type == "mysql":
            mysql_conn = get_mysql_connection(
                os.getenv("MYSQL_DB2_HOST"),
                os.getenv("MYSQL_DB2_PORT", "3306"),
                os.getenv("MYSQL_DB2_USER"),
                os.getenv("MYSQL_DB2_PASSWORD"),
            )
            try:
                if load_type == "incremental" and incremental_col:
                    mysql_to_postgres_incremental(
                        mysql_conn=mysql_conn,
                        postgres_conn=pg_conn,
                        schema_name=target_schema,
                        table_name=table_name,
                        mysql_query=base_sql,
                        batch_size=1_000_000,
                        debug=True,
                    )
                else:
                    mysql_to_postgres_full(
                        mysql_conn=mysql_conn,
                        postgres_conn=pg_conn,
                        schema_name=target_schema,
                        table_name=table_name,
                        mysql_query=base_sql,
                        batch_size=1_000_000,
                        debug=True,
                    )
            finally:
                mysql_conn.close()
        else:
            raise ValueError(f"Unsupported connection type: {conn_type}")

        # -----------------------------
        # Count rows after load
        # -----------------------------
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {target_table}")
            total_rows = cur.fetchone()[0]
            logging.info("[LOAD DEBUG] Total rows now in %s: %d", target_table, total_rows)

        logging.info("[SUCCESS] %s.%s finished", source_schema, table_name)

    except Exception:
        logging.exception("[FAILED] Load task failed: %s.%s", source_schema, table_name)
        raise

    finally:
        pg_conn.close()
        logging.info("[LOAD END] %s.%s", source_schema, table_name)
