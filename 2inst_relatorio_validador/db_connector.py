"""
Unified database connector: Oracle, MySQL, PostgreSQL.
Handles connection, row count with optional period filter, and column schema fetch.
"""

from typing import Optional


DB_TYPES = ["Oracle", "MySQL", "PostgreSQL"]

DEFAULT_PORTS = {"Oracle": 1521, "MySQL": 3306, "PostgreSQL": 5432}


def connect_from_config(cfg: dict):
    """Connect to Oracle using a dict from secrets_manager.get_oracle_config()."""
    return connect(
        "Oracle",
        cfg["host"],
        int(cfg["port"]),
        cfg["service_name"],
        cfg["user"],
        cfg["password"],
    )


def default_port(db_type: str) -> int:
    return DEFAULT_PORTS.get(db_type, 1521)


# ─── Connection factory ───────────────────────────────────────────────────────

def connect(db_type: str, host: str, port: int, database: str, user: str, password: str):
    """Return a DB-API 2.0 connection for the given db_type."""
    if db_type == "Oracle":
        import oracledb
        return oracledb.connect(
            user=user, password=password,
            host=host, port=int(port), service_name=database,
        )
    elif db_type == "MySQL":
        import pymysql
        return pymysql.connect(
            host=host, port=int(port), database=database,
            user=user, password=password, connect_timeout=10,
        )
    elif db_type == "PostgreSQL":
        import psycopg2
        return psycopg2.connect(
            host=host, port=int(port), dbname=database,
            user=user, password=password, connect_timeout=10,
        )
    else:
        raise ValueError(f"Unknown db_type: {db_type}")


def is_available(db_type: str) -> tuple[bool, str]:
    """Check if the required driver is installed."""
    try:
        if db_type == "Oracle":
            import oracledb
        elif db_type == "MySQL":
            import pymysql
        elif db_type == "PostgreSQL":
            import psycopg2
        return True, ""
    except ImportError as e:
        pkg = {"Oracle": "oracledb", "MySQL": "pymysql", "PostgreSQL": "psycopg2-binary"}.get(db_type, "?")
        return False, f"`{pkg}` not installed. Run: pip install {pkg}"


# ─── Period WHERE clause ──────────────────────────────────────────────────────

def build_period_where(db_type: str, date_col: str, period_type: str, period_val: str) -> str:
    if not date_col or period_type == "None" or not period_val:
        return ""
    if period_type == "Month":
        if db_type == "Oracle":
            return f" WHERE TO_CHAR({date_col}, 'YYYY-MM') = '{period_val}'"
        elif db_type == "MySQL":
            return f" WHERE DATE_FORMAT({date_col}, '%Y-%m') = '{period_val}'"
        elif db_type == "PostgreSQL":
            return f" WHERE TO_CHAR({date_col}, 'YYYY-MM') = '{period_val}'"
    elif period_type == "Year":
        if db_type == "Oracle":
            return f" WHERE TO_CHAR({date_col}, 'YYYY') = '{period_val}'"
        elif db_type == "MySQL":
            return f" WHERE YEAR({date_col}) = {period_val}"
        elif db_type == "PostgreSQL":
            return f" WHERE EXTRACT(YEAR FROM {date_col}) = {period_val}"
    return ""


# ─── Operations ───────────────────────────────────────────────────────────────

def count_rows(
    conn, db_type: str, table: str,
    date_col: str = "", period_type: str = "None", period_val: str = "",
) -> Optional[int]:
    where = build_period_where(db_type, date_col, period_type, period_val)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}{where}")
    row = cur.fetchone()
    return int(row[0]) if row else None


def get_columns(conn, db_type: str, table: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE 1=0")
    return [d[0].upper() for d in cur.description]
