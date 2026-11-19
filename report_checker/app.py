"""
Report Quality Checker — Home
Navigate using the sidebar to run checks.
"""

import streamlit as st

st.set_page_config(page_title="Report Quality Checker", layout="wide")

st.title("Report Quality Checker")
st.markdown("""
Use the sidebar to navigate between pages:

| Page | Description |
|------|-------------|
| **AWS Analysis** | Null check, weekly timeline, outlier detection on S3/Iceberg table |
| **Oracle vs AWS** | Row count & schema reconciliation between Oracle source and AWS bronze/ouro |

### Quick start
1. Go to **AWS Analysis** → configure S3 path in sidebar → click **Run checks**
2. Go to **Oracle vs AWS** → set Oracle connection + table names → pick period → click **Compare**

### How checks work
- **Null check** — flags columns that should never be null but are
- **Weekly timeline** — detects weeks with abnormally low row counts (< mean − 2σ)
- **Outlier detection** — IQR fence method; integer columns show integer Q1/Q3, not floats
- **Row reconciliation** — compares Oracle `COUNT(*)` vs S3 parquet row count, optionally filtered by month/year
- **Schema diff** — compares column names between Oracle and AWS
""")
