import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime,date, timedelta
import shutil
import oracledb
import sys
import pandas as pd
import csv
import io
import time
import logging

# ------------------------------------------------------------
# 🔹 Load environment variables
# ------------------------------------------------------------


ENV_PATH = "/opt/parquet/sgdai/dev/dev-joao/utils/.env"

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
    print(f"✅ Loaded environment variables from: {ENV_PATH}")
else:
    print(f"⚠️ .env file not found at: {ENV_PATH}. Using existing environment variables.")

# ------------------------------------------------------------
# 🔹 Spark SQL helper
# ------------------------------------------------------------
def run_query(spark, sql_file_path):
    """
    Reads SQL from a file, executes it using Spark, and returns a DataFrame.
    """
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, "r") as f:
        query = f.read()

    df_result = spark.sql(query)
    # Optional: make column names uppercase
    df_result = df_result.select([F.col(col).alias(col.upper()) for col in df_result.columns])
    return df_result


def oracle_to_postgres_full(
    oracle_conn,
    postgres_conn,
    source_schema: str,
    target_schema: str,
    table_name: str,
    oracle_query: str,
    batch_size: int = 1_000_000,
    debug: bool = True,
):
    target_table = f"{target_schema}.{table_name}"

    ora_cur = oracle_conn.cursor()
    ora_cur.arraysize = batch_size
    ora_cur.prefetchrows = batch_size

    start_time = time.time()

    ora_cur.execute(oracle_query)

    columns = []
    ddl_cols = []
    for col in ora_cur.description:
        col_name = col[0].lower()
        ora_type = col[1]

        if ora_type == oracledb.DB_TYPE_NUMBER:
            pg_type = "NUMERIC"
        elif ora_type in (
            oracledb.DB_TYPE_DATE,
            oracledb.DB_TYPE_TIMESTAMP,
            oracledb.DB_TYPE_TIMESTAMP_TZ,
            oracledb.DB_TYPE_TIMESTAMP_LTZ,
        ):
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"

        columns.append(col_name)
        ddl_cols.append(f"{col_name} {pg_type}")

    column_list = ",".join(columns)

    pg_cur = postgres_conn.cursor()
    pg_cur.execute(
        f"""
        DROP TABLE IF EXISTS {target_table};
        CREATE UNLOGGED TABLE {target_table} (
            {', '.join(ddl_cols)}
        );
        """
    )
    postgres_conn.commit()

    total_rows = 0
    batch_num = 0
    copy_sql = f"""
        COPY {target_table} ({column_list})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
    """

    while True:
        rows = ora_cur.fetchmany(batch_size)
        if not rows:
            break

        batch_num += 1
        total_rows += len(rows)

        buffer = io.StringIO()
        for row in rows:
            buffer.write("\t".join("\\N" if v is None else str(v).replace("\n", " ") for v in row) + "\n")
        buffer.seek(0)

        with postgres_conn.cursor() as c:
            c.copy_expert(copy_sql, buffer)
        postgres_conn.commit()

        if debug:
            elapsed = time.time() - start_time
            logging.info(
                "🚀 Batch %d | Rows %d | Total %d | %.1fs",
                batch_num,
                len(rows),
                total_rows,
                elapsed,
            )

    ora_cur.close()
    pg_cur.close()
    logging.info(
        "✅ Finished full load of %d rows into %s in %.1fs",
        total_rows,
        target_table,
        time.time() - start_time,
    )

# def postgres_to_postgres_full(
#     source_conn,
#     target_conn,
#     source_schema: str,
#     target_schema: str,
#     table_name: str,
#     source_query: str,
#     batch_size: int = 1_000_000,
#     debug: bool = True,
# ):
#     target_table = f"{target_schema}.{source_schema}_{table_name}"

#     src_cur = source_conn.cursor(name="pg_stream_cursor")  # server-side cursor
#     src_cur.itersize = batch_size

#     start_time = time.time()

#     print("description after execute:", src_cur.description)
#     test_row = src_cur.fetchone()
#     print("description after fetchone:", src_cur.description)
#     print("first row:", test_row)

#     print("autocommit:", source_conn.autocommit)

#     # Extract column metadata
#     columns = []
#     ddl_cols = []

#     for col in src_cur.description:
#         col_name = col.name.lower()
#         type_oid = col.type_code

#         # Basic OID → PostgreSQL type mapping
#         if type_oid in (20, 21, 23):  # bigint, smallint, integer
#             pg_type = "BIGINT"
#         elif type_oid in (700, 701, 1700):  # float4, float8, numeric
#             pg_type = "NUMERIC"
#         elif type_oid in (1082,):  # date
#             pg_type = "DATE"
#         elif type_oid in (1114, 1184):  # timestamp, timestamptz
#             pg_type = "TIMESTAMP"
#         elif type_oid in (16,):  # boolean
#             pg_type = "BOOLEAN"
#         else:
#             pg_type = "TEXT"

#         columns.append(col_name)
#         ddl_cols.append(f"{col_name} {pg_type}")

#     column_list = ",".join(columns)

#     tgt_cur = target_conn.cursor()
#     tgt_cur.execute(
#         f"""
#         DROP TABLE IF EXISTS {target_table};
#         CREATE UNLOGGED TABLE {target_table} (
#             {', '.join(ddl_cols)}
#         );
#         """
#     )
#     target_conn.commit()

#     total_rows = 0
#     batch_num = 0

#     copy_sql = f"""
#         COPY {target_table} ({column_list})
#         FROM STDIN
#         WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
#     """

#     while True:
#         rows = src_cur.fetchmany(batch_size)
#         if not rows:
#             break

#         batch_num += 1
#         total_rows += len(rows)

#         buffer = io.StringIO()
#         for row in rows:
#             buffer.write(
#                 "\t".join(
#                     "\\N" if v is None else str(v).replace("\n", " ")
#                     for v in row
#                 )
#             )
    
def postgres_to_postgres_full(
    source_conn,
    target_conn,
    source_schema: str,
    target_schema: str,
    table_name: str,
    source_query: str,
    batch_size: int = 1_000_000,
    debug: bool = True,
):
    target_table = f"{target_schema}.{source_schema}_{table_name}"

    # Server-side cursor for streaming large datasets
    src_cur = source_conn.cursor(name="pg_stream_cursor")
    src_cur.itersize = batch_size

    start_time = time.time()

    src_cur.execute(source_query)

    # ---------------------------------------------------------
    # Extract column metadata
    # ---------------------------------------------------------
    # IMPORTANT: for named cursor, fetch one row first
    first_row = src_cur.fetchone()

    if src_cur.description is None:
        raise ValueError("Source query returned no columns.")

    columns = []
    ddl_cols = []

    for col in src_cur.description:
        col_name = col.name.lower()
        type_oid = col.type_code

        # Basic OID → PostgreSQL type mapping
        if type_oid in (20, 21, 23):  # bigint, smallint, integer
            pg_type = "BIGINT"
        elif type_oid in (700, 701, 1700):  # float4, float8, numeric
            pg_type = "NUMERIC"
        elif type_oid in (1082,):  # date
            pg_type = "DATE"
        elif type_oid in (1114, 1184):  # timestamp
            pg_type = "TIMESTAMP"
        elif type_oid in (16,):  # boolean
            pg_type = "BOOLEAN"
        else:
            pg_type = "TEXT"

        columns.append(col_name)
        ddl_cols.append(f"{col_name} {pg_type}")

    column_list = ",".join(columns)

    # ---------------------------------------------------------
    # Create target table
    # ---------------------------------------------------------
    with target_conn.cursor() as tgt_cur:
        tgt_cur.execute(
            f"""
            DROP TABLE IF EXISTS {target_table};
            CREATE UNLOGGED TABLE {target_table} (
                {', '.join(ddl_cols)}
            );
            """
        )
    target_conn.commit()

    total_rows = 0
    batch_num = 0

    copy_sql = f"""
        COPY {target_table} ({column_list})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
    """

    # If we already fetched first_row, process it first
    pending_rows = []
    if first_row:
        pending_rows.append(first_row)

    # ---------------------------------------------------------
    # Batch loop (same pattern as Oracle version)
    # ---------------------------------------------------------
    while True:
        rows = src_cur.fetchmany(batch_size)

        if not rows and not pending_rows:
            break

        batch_rows = pending_rows + rows
        pending_rows = []

        batch_num += 1
        total_rows += len(batch_rows)

        buffer = io.StringIO()

        for row in batch_rows:
            buffer.write(
                "\t".join(
                    "\\N" if v is None else str(v).replace("\n", " ")
                    for v in row
                )
                + "\n"
            )

        buffer.seek(0)

        with target_conn.cursor() as c:
            c.copy_expert(copy_sql, buffer)

        target_conn.commit()

        if debug:
            elapsed = time.time() - start_time
            logging.info(
                "🚀 Batch %d | Rows %d | Total %d | %.1fs",
                batch_num,
                len(batch_rows),
                total_rows,
                elapsed,
            )

    src_cur.close()

    logging.info(
        "✅ Finished full load of %d rows into %s in %.1fs",
        total_rows,
        target_table,
        time.time() - start_time,
    )
    
def postgres_to_postgres_incremental(
    source_conn,
    target_conn,
    source_schema: str,
    target_schema: str,
    table_name: str,
    source_query: str,
    batch_size: int = 100_000,
    debug: bool = True,
):
    """
    Incremental load from Postgres → Postgres using COPY.
    Appends rows to the target Postgres table.
    """

    target_table = f"{target_schema}.{source_schema}_{table_name}"
    start_time = time.time()
    total_rows = 0
    batch_num = 0

    # Server-side cursor (important for large datasets)
    src_cur = source_conn.cursor(name="pg_incremental_cursor")
    src_cur.itersize = batch_size
    src_cur.execute(source_query)

    columns = [col.name.lower() for col in src_cur.description]
    column_list = ",".join(columns)

    copy_sql = f"""
        COPY {target_table} ({column_list})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
    """

    while True:
        rows = src_cur.fetchmany(batch_size)
        if not rows:
            break

        batch_num += 1
        total_rows += len(rows)

        buffer = io.StringIO()
        for row in rows:
            buffer.write(
                "\t".join(
                    "\\N" if v is None else str(v).replace("\n", " ")
                    for v in row
                )
                + "\n"
            )
        buffer.seek(0)

        with target_conn.cursor() as tgt_cur:
            tgt_cur.copy_expert(copy_sql, buffer)

        target_conn.commit()

        if debug:
            elapsed = time.time() - start_time
            logging.info(
                "🚀 Batch %d | Rows %d | Total loaded %d | %.1fs elapsed",
                batch_num,
                len(rows),
                total_rows,
                elapsed,
            )

    src_cur.close()

    logging.info(
        "✅ Finished incremental load of %d rows into %s in %.1fs",
        total_rows,
        target_table,
        time.time() - start_time,
    )

def oracle_to_postgres_incremental(
    oracle_conn,
    postgres_conn,
    source_schema: str,
    target_schema: str,
    table_name: str,
    oracle_query: str,
    batch_size: int = 100_000,
    debug: bool = True,
):
    """
    Incremental load from Oracle → Postgres using COPY.
    Appends rows to the target Postgres table.

    Arguments:
        oracle_conn: Oracle connection
        postgres_conn: Postgres connection
        source_schema: Source schema in Oracle
        target_schema: Target schema in Postgres
        table_name: Table name in both Oracle and Postgres
        oracle_query: SQL query to fetch incremental rows
        batch_size: Number of rows per batch
        debug: Print batch info
    """
    target_table = f"{target_schema}.{table_name}" 
    start_time = time.time()
    total_rows = 0
    batch_num = 0

    ora_cur = oracle_conn.cursor()
    ora_cur.arraysize = batch_size
    ora_cur.prefetchrows = batch_size
    ora_cur.execute(oracle_query)

    columns = [col[0].lower() for col in ora_cur.description]
    column_list = ",".join(columns)

    copy_sql = f"""
        COPY {target_table} ({column_list})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
    """

    while True:
        rows = ora_cur.fetchmany(batch_size)
        if not rows:
            break

        batch_num += 1
        total_rows += len(rows)

        buffer = io.StringIO()
        for row in rows:
            buffer.write("\t".join("\\N" if v is None else str(v).replace("\n", " ") for v in row) + "\n")
        buffer.seek(0)

        with postgres_conn.cursor() as pg_cur:
            pg_cur.copy_expert(copy_sql, buffer)
        postgres_conn.commit()

        if debug:
            elapsed = time.time() - start_time
            logging.info(
                "🚀 Batch %d | Rows %d | Total loaded %d | %.1fs elapsed",
                batch_num,
                len(rows),
                total_rows,
                elapsed,
            )

    ora_cur.close()
    logging.info(
        "✅ Finished incremental load of %d rows into %s in %.1fs",
        total_rows,
        target_table,
        time.time() - start_time,
    )


def mysql_to_postgres_full(
    mysql_conn,
    postgres_conn,
    mysql_query: str,
    schema_name: str,
    table_name: str,
    batch_size: int = 1_000_000,
    debug: bool = True
):
    """
    FAST MySQL → PostgreSQL using COPY

    Target table:
        temp2inst.(schema_name)_(table_name)
    """

    my_cur = mysql_conn.cursor()
    pg_cur = postgres_conn.cursor()

    target_table = f"temp2inst.{schema_name}_{table_name}"

    start_time = time.time()

    # ----------------------------
    # Execute MySQL query
    # ----------------------------
    my_cur.execute(mysql_query)

    # ----------------------------
    # Build PostgreSQL DDL from MySQL metadata
    # ----------------------------
    columns = []
    ddl_cols = []

    for col in my_cur.description:
        col_name = col[0].lower()
        mysql_type = col[1]

        # MySQL → PostgreSQL type mapping (generic & safe)
        if mysql_type in (1, 2, 3, 8, 9):  # TINY, SHORT, LONG, LONGLONG, INT24
            pg_type = "BIGINT"
        elif mysql_type in (0, 4, 5):  # DECIMAL, FLOAT, DOUBLE
            pg_type = "NUMERIC"
        elif mysql_type in (7, 10, 11, 12):  # TIMESTAMP, DATE, TIME, DATETIME
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"

        columns.append(col_name)
        ddl_cols.append(f"{col_name} {pg_type}")

    column_list = ",".join(columns)

    # ----------------------------
    # Create schema + UNLOGGED table
    # ----------------------------
    pg_cur.execute(
        f"""
        DROP TABLE IF EXISTS {target_table};

        CREATE UNLOGGED TABLE {target_table} (
            {", ".join(ddl_cols)}
        );
        """
    )
    postgres_conn.commit()

    if debug:
        print(f"✅ Created table {target_table}")

    # ----------------------------
    # COPY data in batches
    # ----------------------------
    total_rows = 0
    batch_num = 0

    copy_sql = f"""
        COPY {target_table} ({column_list})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
    """

    while True:
        rows = my_cur.fetchmany(batch_size)
        if not rows:
            break

        batch_num += 1
        total_rows += len(rows)

        buffer = io.StringIO()
        for row in rows:
            buffer.write(
                "\t".join(
                    "\\N" if v is None else str(v).replace("\n", " ")
                    for v in row
                )
                + "\n"
            )
        buffer.seek(0)

        with postgres_conn.cursor() as c:
            c.copy_expert(copy_sql, buffer)

        postgres_conn.commit()

        if debug:
            elapsed = time.time() - start_time
            print(
                f"🚀 Batch {batch_num} | Rows {len(rows)} | "
                f"Total {total_rows} | {elapsed:.1f}s"
            )

    my_cur.close()
    pg_cur.close()

    print(
        f"✅ Finished loading {total_rows} rows into {target_table} "
        f"in {time.time() - start_time:.1f}s"
    )

import io
import time
import logging

def mysql_to_postgres_incremental(
    mysql_conn,
    postgres_conn,
    schema_name: str,
    table_name: str,
    mysql_query: str,
    batch_size: int = 100_000,
    debug: bool = True,
):
    """
    Incremental load from MySQL → PostgreSQL using COPY.
    Appends rows to existing Postgres table.

    Arguments:
        mysql_conn: MySQL connection
        postgres_conn: Postgres connection
        schema_name: Target schema in Postgres
        table_name: Target table in Postgres
        mysql_query: SQL query to fetch incremental rows from MySQL
        batch_size: Number of rows per batch
        debug: Print batch info
    """

    target_table = f"{schema_name}_{table_name}"  # adjust if you want a temp prefix
    start_time = time.time()
    total_rows = 0
    batch_num = 0

    my_cur = mysql_conn.cursor()
    my_cur.arraysize = batch_size

    # Execute incremental query
    my_cur.execute(mysql_query)

    # Build column list from MySQL metadata
    columns = [col[0].lower() for col in my_cur.description]
    column_list = ",".join(columns)

    # ----------------------------
    # COPY data in batches
    # ----------------------------
    copy_sql = f"""
        COPY {target_table} ({column_list})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')
    """

    while True:
        rows = my_cur.fetchmany(batch_size)
        if not rows:
            break

        batch_num += 1
        total_rows += len(rows)

        buffer = io.StringIO()
        for row in rows:
            buffer.write("\t".join("\\N" if v is None else str(v).replace("\n", " ") for v in row) + "\n")
        buffer.seek(0)

        with postgres_conn.cursor() as pg_cur:
            pg_cur.copy_expert(copy_sql, buffer)
        postgres_conn.commit()

        if debug:
            elapsed = time.time() - start_time
            logging.info(
                "🚀 Batch %d | Rows %d | Total loaded %d | %.1fs elapsed",
                batch_num,
                len(rows),
                total_rows,
                elapsed,
            )

    my_cur.close()
    logging.info(
        "✅ Finished incremental load of %d rows into %s in %.1fs",
        total_rows,
        target_table,
        time.time() - start_time,
    )


def build_oracle_select(schema_name: str, table_name: str, columns: list[str]) -> str:
    """
    Build a SELECT statement dynamically from JSON config.
    """
    col_list = ",\n        ".join(columns)

    return f"""
        SELECT
            {col_list}
        FROM {schema_name}.{table_name}
    """


def drop_postgres_table(conn, schema: str, table: str, cascade: bool = False):
    """
    Drop a single PostgreSQL table.

    Example:
        drop_postgres_table(conn, "temp2inst", "ejud_t_processo")
    """
    drop_sql = f"""
        DROP TABLE IF EXISTS {schema}.{table}
        {'CASCADE' if cascade else ''};
    """

    with conn.cursor() as cur:
        cur.execute(drop_sql)
        conn.commit()

    print(f"🗑️ Dropped table {schema}.{table}")

# ---------------------------
# Load input parquet tables
# ---------------------------
def register_parquet_views(spark, tables: dict):
    for view_name, path in tables.items():
        print(f"📄 Registering {view_name} from {path}")

        df = spark.read.option("pathGlobFilter", "*.parquet").parquet(path)
        
        df.createOrReplaceTempView(view_name)
        print(f"✔ TempView created: {view_name}")

# --------------
# Read sql files
# --------------
def read_sql_file(file_path):
    """Reads SQL text from a file and returns it as a string."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ SQL file not found: {file_path}")
    with open(file_path, "r") as f:
        return f.read()

def read_sql_file_dth(file_path, year=None, month=None, day=None):
    """
    Reads SQL text from a file.
    If 'v_dth_referencia' is found, replaces it with
    TO_DATE('YYYY-MM-DD 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    using the provided year/month/day.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ SQL file not found: {file_path}")

    with open(file_path, "r") as f:
        sql = f.read()

    # Only replace if placeholder exists
    if "v_dth_referencia" in sql:
        if year is None or month is None or day is None:
            raise ValueError(
                "❌ year, month and day must be provided when SQL uses v_dth_referencia"
            )

        dth = f"{year:04d}-{month:02d}-{day:02d} 23:59:59"

        sql = sql.replace(
            "v_dth_referencia",
            f"TO_DATE('{dth}', 'YYYY-MM-DD HH24:MI:SS')"
        )

        print(f"📅 v_dth_referencia replaced with {dth}")

    return sql

# ---------------------------
# Execute SQL and save parquetls
# ---------------------------
def run_transform(spark, sql_path, output_path):
    print(f"🧠 Running transformation from SQL: {sql_path}")

    # Read SQL file
    with open(sql_path, "r") as f:
        query = f.read()

    # Run SQL in Spark
    df = spark.sql(query)
    row_count = df.count()
    print(f"🧮 Row count: {row_count}")

    if row_count == 0:
        print("⚠️ No rows returned from query — nothing to write.")
        return

    # ✅ Let Spark handle overwrite safely
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Output saved to {output_path}")
    except Exception as e:
        print(f"❌ Write failed: {e}")
        raise

    df.show(truncate=False)


# ----------------
# Read the parquet
# ----------------
def read_parquet(spark, output_path):
    print(f"📂 Reading Parquet from: {output_path}")
    df = spark.read.parquet(output_path)
    columns = df.columns
    print(f"✅ Columns: {columns}")
    return df, columns



def run_transform_date(
    spark,
    sql_path,
    output_path,
    year,
    month,
    day,
    hour=23,
    minute=59,
    second=59,
):
    """
    Runs a Spark SQL transformation using a SQL file with a date placeholder (v_dth_referencia).
    If hour/minute/second are not provided, defaults to 23:59:59.
    """

    print(f"🧠 Running transformation from SQL: {sql_path}")

    # Build datetime with defaults or provided time
    input_datetime = datetime(year, month, day, hour, minute, second)
    input_date_str = input_datetime.strftime('%Y-%m-%d %H:%M:%S')

    print(f"📅 Using reference datetime: {input_date_str}")

    with open(sql_path, "r") as f:
        query = f.read()

    # Replace placeholder variable in SQL
    query = query.replace(
        "v_dth_referencia",
        f"TIMESTAMP('{input_date_str}')"
    )

    # ✅ PRINT the final SQL
    print("\n📄 ---------- SQL TO EXECUTE ----------")
    print(query)
    print("📄 -------------------------------------------\n")

    try:
        df = spark.sql(query)
        row_count = df.count()
        print(f"🧮 Row count: {row_count}")

        if row_count == 0:
            print("⚠️ No rows returned from query — nothing to write.")
            return

        df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Output saved to {output_path}")

        df.show(truncate=False)

    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise


def run_transform_date_input_output(
    spark,
    sql_path,
    input_parquet,
    output_path,
    year,
    month,
    day,
    table_name="input_table"
):
    """
    Runs a Spark SQL transformation using:
    - A SQL file that contains a placeholder variable 'v_dth_referencia'
    - A Parquet file loaded as input

    Parameters:
        spark          : SparkSession
        sql_path       : path to SQL file
        input_parquet  : input parquet file or folder
        output_path    : destination parquet
        year, month, day : reference date for variable substitution
        table_name     : name of temp view registered in Spark SQL
    """

    print(f"🧠 Running transformation from SQL: {sql_path}")
    print(f"📂 Loading input parquet: {input_parquet}")

    # Load the parquet as DataFrame
    df_input = spark.read.parquet(input_parquet)

    # Register as SQL temporary view
    df_input.createOrReplaceTempView(table_name)

    print(f"📄 Registered temp view: {table_name}")

    # Build datetime reference
    input_datetime = datetime(year, month, day, 23, 59, 59)
    input_date_str = input_datetime.strftime('%Y-%m-%d %H:%M:%S')

    print(f"📅 Using reference datetime: {input_date_str}")

    # Load SQL file
    with open(sql_path, "r") as f:
        query = f.read()

    # Replace placeholder variable in SQL
    query = query.replace("v_dth_referencia", f"TIMESTAMP('{input_date_str}')")

    # Print the SQL to be executed
    print("\n📄 ---------- SQL TO EXECUTE ----------")
    print(query)
    print("📄 -----------------------------------\n")

    try:
        # Execute the SQL
        df = spark.sql(query)
        row_count = df.count()

        print(f"🧮 Row count: {row_count}")

        if row_count == 0:
            print("⚠️ No rows returned — nothing to write.")
            return

        # Save result
        df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Output saved to {output_path}")

        # Optional show
        #df.show(truncate=False)

    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise

# ----------------
# Create tmp view
# ----------------
def create_temp_view(spark, sql_path, year, month, day, view_name):
    """
    Reads a SQL file, replaces parameters, executes the SQL in Spark,
    and creates a temporary view.
    """

    # Read SQL file
    with open(sql_path, "r") as f:
        sql_query = f.read()

    input_datetime = datetime(year, month, day, 23, 59, 59)
    input_date_str = input_datetime.strftime('%Y-%m-%d %H:%M:%S')
    print(f"📅 Using reference datetime: {input_date_str}")

    # Replace placeholder variable in SQL
    sql_query = sql_query.replace("v_dth_referencia", f"TIMESTAMP('{input_date_str}')")

    # Execute SQL
    df = spark.sql(sql_query)

    # Create temp view
    df.createOrReplaceTempView(view_name)

    print(f"Temp view '{view_name}' created successfully.")

# --------------------------
# Gett connetion with Oracle
# --------------------------

def get_oracle_tj01_connection():
    """Return Oracle connection using environment variables."""
    user = os.getenv("ORACLE_TJ01_DB_USER")
    password = os.getenv("ORACLE_TJ01_DB_PASSWORD")
    dsn = os.getenv("ORACLE_TJ01_DB_DSN")  # e.g., "host:1521/service_name"

    if not all([user, password, dsn]):
        raise ValueError("One or more Oracle credentials are missing in .env")

    return oracledb.connect(
        user=user,
        password=password,
        dsn=dsn
    )

# --------------------------
# Resolve run date for today
# --------------------------

def resolve_run_date_yesterday():
    """
    Priority:
    1) CLI arguments: year month day
    2) Default: yesterday
    """
    if len(sys.argv) == 4:
        try:
            year = int(sys.argv[1])
            month = int(sys.argv[2])
            day = int(sys.argv[3])
            return year, month, day
        except ValueError:
            print("❌ Invalid arguments. Expected: year month day (integers).")
            sys.exit(1)

    # Default: yesterday
    run_date = date.today() - timedelta(days=1)
    return run_date.year, run_date.month, run_date.day

    # --------------------------
    # Execute oracle query
    # --------------------------

def execute_query_to_df(engine, query: str) -> pd.DataFrame:
    """
    Execute a SQL query using SQLAlchemy and return a pandas DataFrame.
    """
    return pd.read_sql(query, engine)


def insert_df_postgres(df, table_name, conn, page_size=1000):
    cols = list(df.columns)

    sql = f"""
        INSERT INTO {table_name} ({','.join(cols)})
        VALUES %s
    """

    values = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            values,
            page_size=page_size
        )

    conn.commit()