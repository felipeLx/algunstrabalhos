from modulo.conexoes import get_postgres_connection, get_oracle_connection, test_postgres, test_oracle
from modulo.engine import create_postgres_engine, create_oracle_engine
from modulo.utils import read_sql_file, oracle_to_postgres_batch, oracle_to_postgres_fast, oracle_to_postgres_el, oracle_to_postgres_faster, build_oracle_select
import os
from dotenv import load_dotenv
import sys
import pandas as pd
import time
import json

def main():

    start_time = time.perf_counter()

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(BASE_DIR, "table_config.json")

    load_dotenv("/opt/parquet/projetos/.env")

    ORACLE_TJ01_DB_USER = os.getenv("ORACLE_TJ01_DB_USER")
    ORACLE_TJ01_DB_PASSWORD = os.getenv("ORACLE_TJ01_DB_PASSWORD")
    ORACLE_TJ01_DB_DSN = os.getenv("ORACLE_TJ01_DB_DSN")

    POSTGRESQL_HOST=os.getenv("POSTGRESQL_SL_HOST"),
    POSTGRESQL_PORT=os.getenv("POSTGRESQL_SL_PORT", "5432"),
    POSTGRESQL_DBNAME=os.getenv("POSTGRESQL_SL_DBNAME"),
    POSTGRESQL_USER=os.getenv("POSTGRESQL_SL_USER"),
    POSTGRESQL_PASSWORD=os.getenv("POSTGRESQL_SL_PASSWORD")

    oracle_conn = get_oracle_connection(ORACLE_TJ01_DB_USER, ORACLE_TJ01_DB_PASSWORD, ORACLE_TJ01_DB_DSN)

    postgres_conn = get_postgres_connection(POSTGRESQL_HOST, POSTGRESQL_PORT, POSTGRESQL_DBNAME, POSTGRESQL_USER, POSTGRESQL_PASSWORD)

    query_processo_ejud_sql = read_sql_file("/opt/parquet/projetos/2inst_inspecao/sql/processo_ejud.sql")

    # Load table config
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    for tbl in config["tables"]:
        schema_name = tbl["schema_name"]
        table_name = tbl["table_name"]
        columns = tbl["columns"]

        print(f"\n🚀 Loading {schema_name}.{table_name}")

        oracle_query = build_oracle_select(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns
        )

        oracle_to_postgres_fast(
            oracle_conn=oracle_conn,
            postgres_conn=postgres_conn,
            oracle_query=oracle_query,
            schema_name=schema_name,
            table_name=table_name,
            batch_size=1_000_000,
            debug=True
        )

    #drop_postgres_table(postgres_conn, "ejud", "t_processo")

    elapsed = time.perf_counter() - start_time

    print(f"⏱️ finished in {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()