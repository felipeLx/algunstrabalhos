from dotenv import load_dotenv
load_dotenv("/opt/parquet/projetos/.env")
import os
import psycopg2
import oracledb
import mysql.connector
from mysql.connector import Error

def get_postgres_connection(host, port, dbname, user, password):
    """Return PostgreSQL connection using provided parameters."""
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

def get_oracle_connection(user,password,dsn):
    """Return Oracle connection using environment variables."""

    if not all([user, password, dsn]):
        raise ValueError("One or more Oracle credentials are missing in .env")

    return oracledb.connect(
        user=user,
        password=password,
        dsn=dsn
    )

def get_mysql_connection(host, port, user, password):
    try:
        return mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
        )
    except Error as e:
        raise RuntimeError(f"MySQL connection failed: {e}")

def test_postgres():
    print("Testing PostgreSQL connection...")
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        print("✅ PostgreSQL connection successful:", result)
        cur.close()
        conn.close()
    except Exception as e:
        print("❌ PostgreSQL connection failed:")
        print(e)


def test_oracle():
    print("Testing Oracle TJ01 connection...")
    try:
        conn = get_oracle_tj01_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dual")
        result = cur.fetchone()
        print("✅ Oracle connection successful:", result)
        cur.close()
        conn.close()
    except Exception as e:
        print("❌ Oracle connection failed:")
        print(e)