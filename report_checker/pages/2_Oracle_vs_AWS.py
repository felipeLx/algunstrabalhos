"""
Oracle vs AWS — Row count and schema reconciliation with optional period filter.
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import s3fs
import yaml
from pathlib import Path
from datetime import datetime

try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

st.set_page_config(page_title="Oracle vs AWS", layout="wide")


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


cfg = load_config()
oracle_cfg = cfg.get("oracle", {})


# ─── Sidebar — connections ─────────────────────────────────────────────────────

with st.sidebar:
    st.title("Connections")

    st.subheader("Oracle")
    ora_host = st.text_input("Host", value=oracle_cfg.get("host", ""))
    ora_port = st.number_input("Port", value=int(oracle_cfg.get("port", 1521)), step=1)
    ora_service = st.text_input("Service name", value=oracle_cfg.get("service_name", ""))
    ora_user = st.text_input("User", value=oracle_cfg.get("user", ""))
    ora_pwd_env = st.text_input(
        "Password env var", value=oracle_cfg.get("password_env", "ORACLE_PASSWORD")
    )

    st.subheader("AWS")
    aws_profile = st.text_input("AWS Profile (blank = default)", value=cfg.get("aws_profile", ""))
    s3_bucket = st.text_input("S3 Bucket", value=cfg.get("s3_bucket", "sgdai-data-lake"))


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_oracle_conn():
    password = os.environ.get(ora_pwd_env, "")
    if not password:
        st.error(f"Env var `{ora_pwd_env}` not set. Export it before running.")
        st.stop()
    return oracledb.connect(
        user=ora_user,
        password=password,
        host=ora_host,
        port=int(ora_port),
        service_name=ora_service,
    )


def get_s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(profile=aws_profile) if aws_profile else s3fs.S3FileSystem()


def oracle_count(conn, table: str, date_col: str, period_type: str, period_val: str) -> int | None:
    where = ""
    if date_col and period_type != "None" and period_val:
        if period_type == "Month":
            where = f" WHERE TO_CHAR({date_col}, 'YYYY-MM') = '{period_val}'"
        elif period_type == "Year":
            where = f" WHERE TO_CHAR({date_col}, 'YYYY') = '{period_val}'"
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}{where}")
        row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        st.error(f"Oracle COUNT error: {e}")
        return None


def oracle_columns(conn, table: str) -> list[str]:
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table} WHERE 1=0")
        return [d[0].upper() for d in cur.description]
    except Exception as e:
        st.error(f"Oracle schema error: {e}")
        return []


@st.cache_data(ttl=300, show_spinner="Reading S3 parquet…")
def load_s3(bucket: str, rel_path: str, profile: str, max_rows: int) -> pd.DataFrame:
    fs = s3fs.S3FileSystem(profile=profile) if profile else s3fs.S3FileSystem()
    full = f"{bucket}/{rel_path.strip('/')}"
    files = fs.glob(full + "/**/*.parquet") or fs.glob(full + "/*.parquet")
    if not files:
        return pd.DataFrame()
    dfs, remaining = [], max_rows if max_rows > 0 else None
    for f in sorted(files):
        with fs.open(f, "rb") as fh:
            chunk = pd.read_parquet(fh)
        if remaining is not None:
            chunk = chunk.iloc[:remaining]
            remaining -= len(chunk)
            dfs.append(chunk)
            if remaining <= 0:
                break
        else:
            dfs.append(chunk)
    return pd.concat(dfs, ignore_index=True)


def aws_count(df: pd.DataFrame, date_col: str, period_type: str, period_val: str) -> int:
    if not date_col or period_type == "None" or not period_val or date_col not in df.columns:
        return len(df)
    s = pd.to_datetime(df[date_col], errors="coerce")
    if period_type == "Month":
        mask = s.dt.to_period("M").astype(str) == period_val
    elif period_type == "Year":
        mask = s.dt.year.astype(str) == str(period_val)
    else:
        return len(df)
    return int(mask.sum())


def status_badge(diff: int, diff_pct: float, threshold: float) -> str:
    if diff == 0:
        return "✅ OK"
    if diff_pct <= threshold:
        return "⚠️ WARN"
    return "❌ FAIL"


# ─── Main ─────────────────────────────────────────────────────────────────────

st.title("Oracle vs AWS")

with st.expander("How this page works"):
    st.markdown("""
    Compares row counts and schemas between Oracle source tables and S3/Iceberg tables.

    **Row count reconciliation**
    - Counts rows in Oracle (`SELECT COUNT(*) FROM schema.table`)
    - Loads S3 parquet and counts in memory
    - When a **date column** is configured and a **period** is selected, both counts
      are filtered to the same time window — useful for large tables where a full scan
      would be too slow

    **Schema diff**
    - Fetches Oracle column names (`SELECT * WHERE 1=0`)
    - Compares against parquet column names
    - Shows columns only in Oracle, only in AWS, and shared

    **Oracle table format:** `SCHEMA.TABLE` (e.g. `TJ01.MOVIMENTORELATOR`, `TJ01.EJUD.MOVIMENTO`)
    **AWS path format:** relative path inside the bucket (e.g. `bronze/ejud/autoassociado/movimentorelator`)
    """)

if not ORACLE_AVAILABLE:
    st.error("`oracledb` not installed. Run: `pip install oracledb`")
    st.stop()

# ─── Table pair inputs ────────────────────────────────────────────────────────

st.subheader("Table configuration")

col_ora, col_aws = st.columns(2)
with col_ora:
    oracle_table = st.text_input(
        "Oracle table (SCHEMA.TABLE)",
        placeholder="e.g. TJ01.MOVIMENTORELATOR",
    )
with col_aws:
    aws_rel_path = st.text_input(
        "AWS S3 path (relative to bucket)",
        placeholder="e.g. bronze/ejud/autoassociado/movimentorelator",
    )

# ─── Period filter ────────────────────────────────────────────────────────────

st.subheader("Period filter (optional — recommended for large tables)")

col_dc, col_pt, col_pv, col_thr = st.columns([2, 1, 2, 1])
with col_dc:
    date_col = st.text_input("Date column name", placeholder="e.g. dth_referencia")
with col_pt:
    period_type = st.selectbox("Granularity", ["None", "Month", "Year"])
with col_pv:
    if period_type == "Month":
        now = datetime.now()
        period_val = st.text_input("Period (YYYY-MM)", value=f"{now.year}-{now.month:02d}")
    elif period_type == "Year":
        period_val = st.text_input("Period (YYYY)", value=str(datetime.now().year))
    else:
        period_val = ""
        st.text_input("Period", value="—", disabled=True)
with col_thr:
    threshold_pct = st.number_input("Alert threshold (%)", min_value=0.0, value=0.0, step=0.1,
                                     help="Flag as FAIL only when diff_pct > this value. 0 = any diff.")

max_rows_aws = st.number_input(
    "Max rows to load from AWS (0 = all) — reduce for big tables",
    min_value=0, value=int(cfg.get("max_rows", 500_000)), step=100_000
)

run = st.button("Compare", type="primary")

if not run:
    st.stop()

# ─── Validate ─────────────────────────────────────────────────────────────────

if not oracle_table:
    st.error("Oracle table required.")
    st.stop()
if not aws_rel_path:
    st.error("AWS S3 path required.")
    st.stop()
if not ora_host or not ora_service or not ora_user:
    st.error("Oracle connection settings incomplete (check sidebar).")
    st.stop()

# ─── Run comparison ───────────────────────────────────────────────────────────

tab_count, tab_schema = st.tabs(["Row Count", "Schema Diff"])

with st.spinner("Connecting to Oracle…"):
    try:
        conn = get_oracle_conn()
    except Exception as e:
        st.error(f"Oracle connection failed: {e}")
        st.stop()

with st.spinner("Loading AWS parquet…"):
    df_aws = load_s3(s3_bucket, aws_rel_path, aws_profile, int(max_rows_aws))
    if df_aws.empty:
        st.error(f"No parquet files found at `s3://{s3_bucket}/{aws_rel_path.strip('/')}/`")
        st.stop()

st.success(
    f"AWS: loaded **{len(df_aws):,}** rows × **{len(df_aws.columns)}** columns "
    f"from `s3://{s3_bucket}/{aws_rel_path.strip('/')}/`"
)

# ── Row count tab ──────────────────────────────────────────────────────────────
with tab_count:
    st.subheader("Row Count Reconciliation")

    period_label = ""
    if period_type != "None" and period_val:
        period_label = f" [{period_type}: {period_val}]"

    with st.spinner("Counting Oracle rows…"):
        ora_cnt = oracle_count(conn, oracle_table, date_col, period_type, period_val)

    aws_cnt = aws_count(df_aws, date_col, period_type, period_val)

    if ora_cnt is None:
        st.error("Could not count Oracle rows.")
    else:
        diff = aws_cnt - ora_cnt
        diff_pct = round(abs(diff) / ora_cnt * 100, 4) if ora_cnt else 0.0
        status = status_badge(diff, diff_pct, threshold_pct)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric(f"Oracle{period_label}", f"{ora_cnt:,}")
        c2.metric(f"AWS{period_label}", f"{aws_cnt:,}")
        c3.metric("Diff (AWS − Oracle)", f"{diff:+,}")
        c4.metric("Diff %", f"{diff_pct:.4f}%")
        c5.metric("Status", status)

        if max_rows_aws > 0 and aws_cnt >= max_rows_aws:
            st.warning(
                f"AWS row count hit the **max rows limit ({max_rows_aws:,})**. "
                "The actual count may be higher. Set max rows = 0 to load all."
            )

        if diff != 0:
            st.markdown("**Possible causes:**")
            causes = []
            if diff > 0:
                causes += [
                    "AWS has more rows — possible duplicates introduced in ETL",
                    "Oracle data was deleted/archived after extraction",
                ]
            else:
                causes += [
                    "AWS has fewer rows — partial load or extraction failure",
                    "Oracle has rows inserted after last pipeline run",
                ]
            for c in causes:
                st.markdown(f"- {c}")

# ── Schema diff tab ────────────────────────────────────────────────────────────
with tab_schema:
    st.subheader("Schema Comparison")

    with st.spinner("Fetching Oracle schema…"):
        ora_cols = oracle_columns(conn, oracle_table)

    aws_cols = [c.upper() for c in df_aws.columns.tolist()]

    ora_set = set(ora_cols)
    aws_set = set(aws_cols)

    only_oracle = sorted(ora_set - aws_set)
    only_aws = sorted(aws_set - ora_set)
    shared = sorted(ora_set & aws_set)

    c1, c2, c3 = st.columns(3)
    c1.metric("Oracle columns", len(ora_cols))
    c2.metric("AWS columns", len(aws_cols))
    c3.metric("Shared", len(shared))

    if only_oracle:
        st.error(f"**{len(only_oracle)} column(s) only in Oracle** (missing from AWS):")
        st.dataframe(pd.DataFrame({"column": only_oracle}), use_container_width=True)
    else:
        st.success("No columns missing from AWS.")

    if only_aws:
        st.warning(f"**{len(only_aws)} column(s) only in AWS** (not in Oracle source):")
        st.dataframe(pd.DataFrame({"column": only_aws}), use_container_width=True)

    with st.expander(f"Shared columns ({len(shared)})"):
        st.dataframe(pd.DataFrame({"column": shared}), use_container_width=True)

conn.close()
