import logging
import sys
import os
from psycopg2 import sql

from modulo.conexoes import get_postgres_connection

# --------------------------------------------------
# Constants
# --------------------------------------------------
TIMEZONE = "America/Sao_Paulo"

# --------------------------------------------------
# Logging (Airflow-friendly)
# --------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def build_source_select(schema: str, table: str, columns: list[str]) -> sql.SQL:
    """
    Build a SELECT that guarantees column names and order.
    """
    return sql.SQL("SELECT {} FROM {}.{}").format(
        sql.SQL(", ").join(
            sql.SQL("{} AS {}").format(
                sql.Identifier(col),
                sql.Identifier(col),
            )
            for col in columns
        ),
        sql.Identifier(schema),
        sql.Identifier(table),
    )


def build_column_alias_list(columns: list[str]) -> sql.SQL:
    return sql.SQL(", ").join(sql.Identifier(c) for c in columns)


def normalize_sources(source_schema: list[str], source_table: list[str]):
    """
    Supports:
        - 1 schema + many tables
        - N schemas + N tables (1:1)
    """
    if not source_schema or not source_table:
        raise ValueError("source_schema and source_table cannot be empty")

    # Case 1: single schema, multiple tables
    if len(source_schema) == 1:
        return [(source_schema[0], table) for table in source_table]

    # Case 2: 1:1 mapping
    if len(source_schema) == len(source_table):
        return list(zip(source_schema, source_table))

    raise ValueError(
        "Invalid source configuration: provide either "
        "1 schema for many tables OR matching schema/table pairs"
    )


# --------------------------------------------------
# Production runner
# --------------------------------------------------
def run_production_task(task_config: dict, production_config: dict, **context):
    """
    Production task:
      - MERGE multiple sources into a target table
      - All sources must expose identical columns
    """

    task = context.get("task")
    task_id = task.task_id if task else "unknown_task"

    # -----------------------------
    # Logical date (Airflow-safe)
    # -----------------------------
    logical_date = context.get("logical_date") or context.get("execution_date")
    if logical_date is None:
        raise ValueError("logical_date not available in Airflow context")

    logical_date = (
        logical_date
        .in_timezone(TIMEZONE)
        .start_of("hour")
    )

    # -----------------------------
    # Config validation
    # -----------------------------
   
    required_production_keys = {"source_schema", "source_table", "target_schema", "target_table", "columns", "merge_key"}

    missing = required_production_keys - production_config.keys()
    if missing:
        raise ValueError(f"Missing production config keys: {missing}")

    required_task_keys = {"task_id", "task_name"}

    missing = required_task_keys - production_config.keys()
    if missing:
        raise ValueError(f"Missing task config keys: {missing}")

    task_id = task_config["task_id"]
    task_name = task_config.get("task_name", task_id)

    source_schema = production_config["source_schema"]
    source_table = production_config["source_table"]
    target_schema = production_config["target_schema"]
    target_table = production_config["target_table"]
    columns = production_config["columns"]
    merge_key = production_config["merge_key"]
    
    # -----------------------------
    # Logging header
    # -----------------------------
    logging.info("=" * 60)
    logging.info("[PRODUCTION START]")
    logging.info("Task ID           : %s", task_id)
    logging.info("Name              : %s", task_name)
    logging.info("Source Schema     : %s", source_schema)
    logging.info("Source Table      : %s", source_table)
    logging.info("Target Schema     : %s", target_schema)
    logging.info("Target Table      : %s", target_table)
    logging.info("Merge Key         : %s", merge_key)
    logging.info("Columns           : %s", columns)
    logging.info("Logical Date      : %s", logical_date)
    logging.info("=" * 60)

     # -----------------------------
    # Normalize sources
    # -----------------------------
    schema_table_pairs = normalize_sources(source_schema, source_table)

    select_statements = [
        build_source_select(schema, table, columns)
        for schema, table in schema_table_pairs
    ]

    if not select_statements:
        raise ValueError("No source SELECT statements generated")

    using_clause = (
        select_statements[0]
        if len(select_statements) == 1
        else sql.SQL(" UNION ALL ").join(select_statements)
    )

    # -----------------------------
    # SQL fragments
    # -----------------------------
    update_cols = [c for c in columns if c != merge_key]

    set_clause = sql.SQL(", ").join(
        sql.SQL("{} = source.{}").format(
            sql.Identifier(col),
            sql.Identifier(col),
        )
        for col in update_cols
    )

    insert_cols = sql.SQL(", ").join(
        sql.Identifier(col) for col in columns
    )

    insert_values = sql.SQL(", ").join(
        sql.SQL("source.{}").format(sql.Identifier(col))
        for col in columns
    )

    # -----------------------------
    # MERGE SQL
    # -----------------------------
    merge_sql = sql.SQL("""
        MERGE INTO {}.{} AS target
        USING ({}) AS source ({})
        ON target.{} = source.{}
        WHEN MATCHED AND (
            {}
        )
        THEN UPDATE SET
            {}
        WHEN NOT MATCHED THEN
            INSERT ({})
            VALUES ({})
    """).format(
        sql.Identifier(target_schema),
        sql.Identifier(target_table),
        using_clause,
        build_column_alias_list(columns),
        sql.Identifier(merge_key),
        sql.Identifier(merge_key),
        sql.SQL(" OR ").join(
            sql.SQL("target.{} IS DISTINCT FROM source.{}").format(
                sql.Identifier(col),
                sql.Identifier(col),
            )
            for col in update_cols
        ),
        set_clause,
        insert_cols,
        insert_values,
    )

    # -----------------------------
    # Execute
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
            logging.info("[MERGE SQL]\n%s", cur.mogrify(merge_sql).decode())
            cur.execute(merge_sql)
            conn.commit()

            logging.info("[SUCCESS] Production task completed")

    except Exception:
        conn.rollback()
        logging.exception("[FAILED] Production task failed")
        raise

    finally:
        conn.close()
        logging.info("[PRODUCTION END] %s", task_id)
