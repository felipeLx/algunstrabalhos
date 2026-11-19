import os
import sys
import logging
from pathlib import Path
from psycopg2 import sql
import pendulum
import time

from modulo.conexoes import get_postgres_connection

# --------------------------------------------------
# Constants
# --------------------------------------------------
TIMEZONE = "America/Sao_Paulo"
SQL_FOLDER = Path(__file__).parent.parent / "sql"

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# --------------------------------------------------
# Transformation runner
# --------------------------------------------------
def run_transformation(task_config: dict, **context):
    """
    Run a transformation using metadata from etl2inst.etl_tasks

    Expected keys in task_config:
      - source_schema
      - target_schema
      - target_table
    """

    start_time = time.perf_counter()

    # -----------------------------
    # Validate config
    # -----------------------------
    required_keys = {"source_schema", "target_schema", "target_table"}
    missing = required_keys - task_config.keys()
    if missing:
        raise ValueError(f"Missing required transformation config keys: {missing}")

    source_schema = task_config["source_schema"]
    target_schema = task_config["target_schema"]
    table_name = task_config["target_table"]

    task_id = context.get("task").task_id if "task" in context else task_config.get("task_name", "unknown_task")

    # -----------------------------
    # Logical date
    # -----------------------------
    logical_date = context.get("logical_date") or context.get("execution_date")
    if logical_date is None:
        raise ValueError("logical_date not available in Airflow context")

    logical_date = (
        logical_date
        .in_timezone(TIMEZONE)
        .start_of("hour")
    )

    logical_date_str = logical_date.format("YYYY-MM-DD HH:mm:ss")

    # -----------------------------
    # Logging
    # -----------------------------
    logging.info("=" * 60)
    logging.info("[TRANSFORMATION START]")
    logging.info("Task ID       : %s", task_id)
    logging.info("Source schema : %s", source_schema)
    logging.info("Target table  : %s.%s", target_schema, table_name)
    logging.info("Logical date  : %s", logical_date_str)
    logging.info("=" * 60)

    # -----------------------------
    # SQL file
    # -----------------------------
    sql_file = SQL_FOLDER / f"{table_name}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"Transformation SQL not found: {sql_file}")

    base_query = sql_file.read_text()

    # Replace logical date placeholder
    base_query = base_query.replace(
        "{v_dateref_str}",
        logical_date_str,
    )

    # -----------------------------
    # PostgreSQL connection
    # -----------------------------
    conn = get_postgres_connection(
        host=os.getenv("POSTGRESQL_SL_HOST"),
        port=os.getenv("POSTGRESQL_SL_PORT", "5432"),
        dbname=os.getenv("POSTGRESQL_SL_DBNAME"),
        user=os.getenv("POSTGRESQL_SL_USER"),
        password=os.getenv("POSTGRESQL_SL_PASSWORD"),
    )

    try:
        with conn.cursor() as cur:

            # -----------------------------
            # DROP TABLE
            # -----------------------------
            drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(target_schema),
                sql.Identifier(table_name),
            )

            logging.info("[SQL]\n%s", cur.mogrify(drop_query).decode())
            cur.execute(drop_query)

            # -----------------------------
            # CREATE TABLE
            # -----------------------------
            create_query = sql.SQL("""
                CREATE TABLE {}.{} AS
                {}
            """).format(
                sql.Identifier(target_schema),
                sql.Identifier(table_name),
                sql.SQL(base_query),
            )

            logging.info("Starting CREATE TABLE %s.%s", target_schema, table_name)
            logging.info("[SQL]\n%s", cur.mogrify(create_query).decode())

            cur.execute(create_query)

            conn.commit()

            elapsed = time.monotonic() - start_time

            logging.info("=" * 60)
            logging.info("[TRANSFORMATION FINISHED]")
            logging.info("Target table  : %s.%s", target_schema, table_name)
            logging.info(
                "Execution time: %.2f seconds (%.2f minutes)",
                elapsed,
                elapsed / 60,
            )
            logging.info("=" * 60)

    except Exception:
        conn.rollback()
        logging.exception(
            "[FAILED] Transformation failed for %s.%s",
            target_schema,
            table_name,
        )
        raise

    finally:
        conn.close()
        logging.info("[TRANSFORMATION END] %s", task_id)
