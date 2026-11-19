"""
AWS Analysis — Null check, weekly timeline, outlier detection on S3/Iceberg table.
"""

import streamlit as st
import pandas as pd
import numpy as np
import s3fs
import yaml
import os
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

st.set_page_config(page_title="AWS Analysis", layout="wide")


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


def build_s3_path(cfg: dict) -> str:
    parts = [
        cfg.get("s3_bucket", ""),
        cfg.get("s3_prefix", ""),
        cfg.get("s3_projeto", ""),
        cfg.get("s3_tabela", ""),
    ]
    return "s3://" + "/".join(p for p in parts if p) + "/"


def save_config(cfg: dict):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)


cfg = load_config()

# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("AWS Settings")

    st.subheader("S3 Path")
    col1, col2 = st.columns(2)
    with col1:
        bucket = st.text_input("Bucket", value=cfg.get("s3_bucket", "sgdai-data-lake"))
        projeto = st.text_input("Projeto", value=cfg.get("s3_projeto", "dw_2inst_ouro"))
    with col2:
        prefix = st.text_input("Prefix", value=cfg.get("s3_prefix", "ouro"))
        tabela = st.text_input("Tabela", value=cfg.get("s3_tabela", "tb_ft_meta_analitico"))

    st.caption(f"→ `{bucket}/{prefix}/{projeto}/{tabela}/`")

    aws_profile = st.text_input("AWS Profile (blank = default)", value=cfg.get("aws_profile", ""))

    date_col = st.text_input(
        "Date column (for weekly timeline)",
        value=cfg.get("date_col", ""),
        placeholder="e.g. dth_referencia",
    )

    not_null_cols_raw = st.text_area(
        "Not-null columns (one per line)",
        value="\n".join(cfg.get("not_null_cols", [])),
        height=150,
    )
    not_null_cols = [c.strip() for c in not_null_cols_raw.splitlines() if c.strip()]

    outlier_multiplier = st.slider(
        "Outlier IQR multiplier", 1.0, 3.0,
        float(cfg.get("outlier_multiplier", 1.5)), 0.1
    )

    max_rows = st.number_input(
        "Max rows to load (0 = all)", min_value=0,
        value=int(cfg.get("max_rows", 500_000)), step=100_000
    )

    if st.button("Save settings"):
        save_config({
            "s3_bucket": bucket,
            "s3_prefix": prefix,
            "s3_projeto": projeto,
            "s3_tabela": tabela,
            "aws_profile": aws_profile,
            "date_col": date_col,
            "not_null_cols": not_null_cols,
            "outlier_multiplier": outlier_multiplier,
            "max_rows": max_rows,
        })
        st.success("Saved.")

    run = st.button("Run checks", type="primary", use_container_width=True)

# ─── Load data ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Reading S3 parquet…")
def load_parquet(path: str, profile: str, nrows: int) -> pd.DataFrame:
    kwargs = {"profile": profile} if profile else {}
    fs = s3fs.S3FileSystem(**kwargs)

    files = (
        fs.glob(path.rstrip("/") + "/**/*.parquet")
        or fs.glob(path.rstrip("/") + "/*.parquet")
    )
    if not files:
        files = [path.replace("s3://", "")]

    dfs = []
    remaining = nrows if nrows > 0 else None
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


# ─── Check functions ──────────────────────────────────────────────────────────

def check_nulls(df: pd.DataFrame, required_cols: list[str]) -> pd.DataFrame:
    rows = []
    for col in required_cols:
        if col not in df.columns:
            rows.append({"column": col, "status": "MISSING", "null_count": None, "null_pct": None, "total_rows": len(df)})
            continue
        null_count = int(df[col].isna().sum())
        null_pct = round(null_count / len(df) * 100, 2) if len(df) else 0
        rows.append({
            "column": col,
            "status": "OK" if null_count == 0 else "FAIL",
            "null_count": null_count,
            "null_pct": null_pct,
            "total_rows": len(df),
        })
    return pd.DataFrame(rows)


def weekly_timeline(df: pd.DataFrame, date_col: str) -> pd.DataFrame | None:
    if not date_col or date_col not in df.columns:
        return None
    s = pd.to_datetime(df[date_col], errors="coerce")
    week = s.dt.to_period("W").dt.start_time
    return week.value_counts().sort_index().rename_axis("week_start").reset_index(name="row_count")


def detect_outliers(df: pd.DataFrame, k: float) -> pd.DataFrame:
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    rows = []
    for col in numeric:
        s = df[col].dropna()
        if s.empty:
            continue
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        low, high = q1 - k * iqr, q3 + k * iqr
        outlier_mask = (df[col] < low) | (df[col] > high)
        count = int(outlier_mask.sum())

        # respect actual data type — integer columns must not show fake float precision
        is_int = pd.api.types.is_integer_dtype(df[col])
        def fmt(v):
            return int(round(v)) if is_int else round(float(v), 4)

        rows.append({
            "column": col,
            "dtype": str(df[col].dtype),
            "outlier_count": count,
            "outlier_pct": round(count / len(df) * 100, 2) if len(df) else 0,
            "q1": fmt(q1),
            "q3": fmt(q3),
            "iqr": fmt(iqr),
            "lower_fence": fmt(low),
            "upper_fence": fmt(high),
        })
    return pd.DataFrame(rows).sort_values("outlier_count", ascending=False)


def check_duplicates(df: pd.DataFrame, key_cols: list[str]) -> dict:
    if not key_cols or not all(c in df.columns for c in key_cols):
        return {}
    dup_count = int(df.duplicated(subset=key_cols).sum())
    return {"duplicate_rows": dup_count, "total_rows": len(df), "dup_pct": round(dup_count / len(df) * 100, 2) if len(df) else 0}


# ─── Main ─────────────────────────────────────────────────────────────────────

st.title("AWS Analysis")

if not run:
    st.info("Configure settings in the sidebar, then click **Run checks**.")
    st.stop()

try:
    s3_path = build_s3_path({
        "s3_bucket": bucket, "s3_prefix": prefix,
        "s3_projeto": projeto, "s3_tabela": tabela,
    })
    df = load_parquet(s3_path, aws_profile, int(max_rows))
except Exception as e:
    st.error(f"Failed to load data: {e}")
    st.stop()

st.success(f"Loaded **{len(df):,}** rows × **{len(df.columns)}** columns from `{s3_path}`")

tab_null, tab_timeline, tab_outliers, tab_schema = st.tabs([
    "Null Check", "Weekly Timeline", "Outliers", "Schema"
])

# ── Null check ─────────────────────────────────────────────────────────────────
with tab_null:
    st.subheader("Null Check")

    with st.expander("What this check does"):
        st.markdown("""
        Scans each configured column and counts `NULL` / `NaN` values.

        - **OK** — zero nulls found
        - **FAIL** — at least one null found
        - **MISSING** — column does not exist in the table (schema drift)

        **Why it matters:** Null values in key columns (IDs, dates, required metrics)
        indicate broken ETL stages, failed joins, or source extraction issues.
        A single null in `id_processo` means one case is untraceable in reports.
        """)

    if not not_null_cols:
        st.warning("No not-null columns configured. Add them in sidebar.")
    else:
        result = check_nulls(df, not_null_cols)
        fail = result[result["status"] != "OK"]
        ok = result[result["status"] == "OK"]

        c1, c2, c3 = st.columns(3)
        c1.metric("Columns checked", len(result))
        c2.metric("Passed", len(ok))
        c3.metric("Failed / Missing", len(fail), delta_color="inverse")

        if not fail.empty:
            st.error("Some columns failed null check:")
            st.dataframe(
                fail.style.map(
                    lambda v: "background-color: #ffcccc" if v in ("FAIL", "MISSING") else "",
                    subset=["status"]
                ),
                use_container_width=True,
            )
        else:
            st.success("All required columns have zero nulls.")

        with st.expander("Full results"):
            st.dataframe(result, use_container_width=True)

# ── Weekly timeline ────────────────────────────────────────────────────────────
with tab_timeline:
    st.subheader("Weekly Row Count")

    with st.expander("What this check does"):
        st.markdown("""
        Groups rows by calendar week using the configured date column and plots
        the row count per week.

        Flags weeks where the count is **below mean − 2σ** as abnormally low.

        **Why it matters:** A sudden drop in weekly rows signals:
        - Pipeline failure or partial load for that period
        - Data retention/deletion
        - Source system outage during extraction window

        Stable pipelines should have roughly consistent weekly volumes.
        """)

    tl = weekly_timeline(df, date_col)
    if tl is None:
        st.warning("Set a date column in sidebar to enable timeline.")
    else:
        st.line_chart(tl.set_index("week_start")["row_count"])
        mean_count = tl["row_count"].mean()
        std_count = tl["row_count"].std()
        low_weeks = tl[tl["row_count"] < mean_count - 2 * std_count]
        if not low_weeks.empty:
            st.warning(f"**{len(low_weeks)} week(s)** with abnormally low row count (< mean − 2σ):")
            st.dataframe(low_weeks, use_container_width=True)
        else:
            st.success("No weeks with abnormally low counts.")
        st.dataframe(tl, use_container_width=True)

# ── Outliers ───────────────────────────────────────────────────────────────────
with tab_outliers:
    st.subheader(f"Outlier Detection (IQR × {outlier_multiplier})")

    with st.expander("What this check does"):
        st.markdown(f"""
        Uses the **IQR fence method** to flag statistical outliers in every numeric column.

        ```
        lower fence = Q1 − {outlier_multiplier} × IQR
        upper fence = Q3 + {outlier_multiplier} × IQR
        ```

        Any value outside these fences is counted as an outlier.

        **Integer columns** — Q1, Q3, fences shown as integers (no false decimal precision).
        Float columns show 4 decimal places.

        **Why it matters:**
        - Encoding errors often produce extreme values (e.g. 9999 as a sentinel)
        - Calculation bugs in ETL can produce unrealistic metric values
        - Outliers in `qt_acervo` or meta numerators distort aggregated reports

        **Adjust multiplier** in sidebar: lower = stricter, higher = more lenient.
        """)

    out_df = detect_outliers(df, outlier_multiplier)
    if out_df.empty:
        st.info("No numeric columns found.")
    else:
        flagged = out_df[out_df["outlier_count"] > 0]
        st.metric("Columns with outliers", len(flagged))
        st.dataframe(
            out_df.style.background_gradient(subset=["outlier_pct"], cmap="Reds"),
            use_container_width=True,
        )

# ── Schema ─────────────────────────────────────────────────────────────────────
with tab_schema:
    st.subheader("Schema & Sample")

    with st.expander("What this check does"):
        st.markdown("""
        Shows column names, data types, and a sample of raw rows.

        Use this to verify:
        - Columns that should be integer are not stored as float/string
        - No unexpected columns added by pipeline changes
        - Date columns are parsed as datetime, not object/string

        Compare against the **Oracle vs AWS** page to spot schema drift between source and lake.
        """)

    dtypes_df = pd.DataFrame({"column": df.columns, "dtype": df.dtypes.values.astype(str)})

    int_as_float = dtypes_df[
        dtypes_df["dtype"].isin(["float64", "float32"])
        & dtypes_df["column"].str.contains("qt_|id_|nr_|cd_", regex=True)
    ]
    if not int_as_float.empty:
        st.warning(
            f"**{len(int_as_float)} column(s)** look like integers but stored as float "
            "(common after pandas read with nulls). Consider casting in ETL."
        )
        st.dataframe(int_as_float, use_container_width=True)

    st.dataframe(dtypes_df, use_container_width=True)
    st.subheader("Sample rows (first 100)")
    st.dataframe(df.head(100), use_container_width=True)
