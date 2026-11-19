import os
import time
import logging
import sys
from dotenv import load_dotenv

from psycopg2 import sql

from modulo.conexoes import (
    get_oracle_connection,
    get_postgres_connection,
    get_mysql_connection,
)
from modulo.utils import (
    oracle_to_postgres_full,
    mysql_to_postgres_full,
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
# Load environment once
# -------------------------------------------------
load_dotenv("/opt/parquet/projetos/.env")


def run_extract_load(table_config: dict, **context):
    """
    Run ETL for a single table.
    table_config comes directly from JSON.
    """

    logging.info("[%s] Executing FULL LOAD (extract_load.py)", task_name)

    # -----------------------------
    # Validate config
    # -----------------------------
    required_keys = {"connection", "schema_name", "table_name", "columns"}
    missing = required_keys - table_config.keys()
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")

    connection_name = table_config["connection"].lower()
    schema_name = table_config["schema_name"]
    table_name = table_config["table_name"]
    columns = table_config["columns"]

    logging.info("=" * 60)
    logging.info("[LOAD START] %s.%s", schema_name, table_name)
    logging.info("Source type : %s", connection_name)
    logging.info("Columns     : %s", columns)
    logging.info("=" * 60)
    sys.stdout.flush()

    start = time.perf_counter()

    # -----------------------------
    # Postgres (target) connection
    # -----------------------------
    postgres_conn = get_postgres_connection(
        os.getenv("POSTGRESQL_SL_HOST"),
        os.getenv("POSTGRESQL_SL_PORT", "5432"),
        os.getenv("POSTGRESQL_SL_DBNAME"),
        os.getenv("POSTGRESQL_SL_USER"),
        os.getenv("POSTGRESQL_SL_PASSWORD"),
    )

    try:
        # -----------------------------
        # Build SELECT safely (for logging + reuse)
        # -----------------------------
        source_query = sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )

        # IMPORTANT: render SQL using POSTGRES connection
        logging.info("[SOURCE SQL]\n%s", source_query.as_string(postgres_conn))

        # -----------------------------
        # Oracle source
        # -----------------------------
        if connection_name == "oracle":
            oracle_conn = get_oracle_connection(
                os.getenv("ORACLE_TJ01_DB_USER"),
                os.getenv("ORACLE_TJ01_DB_PASSWORD"),
                os.getenv("ORACLE_TJ01_DB_DSN"),
            )

            try:
                oracle_to_postgres_full(
                    oracle_conn=oracle_conn,
                    postgres_conn=postgres_conn,
                    oracle_query=source_query.as_string(postgres_conn),
                    schema_name=schema_name,
                    table_name=table_name,
                    batch_size=1_000_000,
                    debug=True,
                )
            finally:
                oracle_conn.close()

        # -----------------------------
        # MySQL source
        # -----------------------------
        elif connection_name == "mysql":
            mysql_conn = get_mysql_connection(
                os.getenv("MYSQL_DB2_HOST"),
                os.getenv("MYSQL_DB2_PORT", "3306"),
                os.getenv("MYSQL_DB2_USER"),
                os.getenv("MYSQL_DB2_PASSWORD"),
            )

            try:
                mysql_to_postgres_full(
                    mysql_conn=mysql_conn,
                    postgres_conn=postgres_conn,
                    mysql_query=source_query.as_string(postgres_conn),
                    schema_name=schema_name,
                    table_name=table_name,
                    batch_size=1_000_000,
                    debug=True,
                )
            finally:
                mysql_conn.close()

        else:
            raise ValueError(f"Unsupported connection type: {connection_name}")

        elapsed = time.perf_counter() - start
        logging.info(
            "[SUCCESS] %s.%s finished in %.2fs",
            schema_name,
            table_name,
            elapsed,
        )

    except Exception:
        logging.exception("[FAILED] Load task failed: %s.%s", schema_name, table_name)
        raise

    finally:
        postgres_conn.close()
        logging.info("[LOAD END] %s.%s", schema_name, table_name)
