# Report Quality Checker

Data quality gate for S3 parquet reports. Check nulls, weekly consistency, and outliers.

## Setup

```bash
pip install -r requirements.txt
```

**Optional:** if not using Oracle reconciliation, skip `oracledb`:
```bash
pip install streamlit pandas pyarrow s3fs boto3 pyyaml numpy
```

## Configuration

All configuration in `config.yaml`:

```yaml
# S3 path components (assembled into: s3://bucket/prefix/projeto/tabela/)
s3_bucket: sgdai-data-lake
s3_prefix: ouro
s3_projeto: dw_2inst_ouro
s3_tabela: tb_ft_meta_analitico

# AWS
aws_profile: ""  # blank = default / env vars

# Data quality
date_col: dth_referencia
not_null_cols:
  - id_processo
  - dth_referencia

# Processing
outlier_multiplier: 1.5  # IQR fence
max_rows: 500000
```

**Single source of truth:** All scripts read from `config.yaml`. Change once, used everywhere.

## Usage

### Interactive Report (Streamlit)

```bash
streamlit run app.py
```

Opens browser at `http://localhost:8501`.

Features:
- **Null Check** — per-column null counts and percentages
- **Weekly Timeline** — row count trends + anomaly detection
- **Outliers** — IQR-based detection for numeric columns
- **Raw Schema** — column types and sample data

Sidebar: edit S3 path components, not-null columns, date column, outlier threshold. Click "Save settings" to persist.

### CLI Check (CI gate)

```bash
python ci_check.py
```

Reads config.yaml, validates nulls, exits 0 (pass) or 1 (fail). Override with args:

```bash
python ci_check.py --not-null id_processo dth_referencia --max-rows 100000
```

### Bronze vs Oracle Reconciliation (Standalone)

Heavy row-count validation. Run separate from UI:

```bash
export ORACLE_PASSWORD=your_password
python compare_bronze_oracle.py
```

Outputs to console — logs matched/mismatched tables, exit code 0 (pass) or 1 (fail).

Override config with CLI args:

```bash
python compare_bronze_oracle.py --threshold 0.01  # flag diff > 1%
```

**Requires:** Oracle connection details in config.yaml (`oracle:` block) + `bronze_tables:` list.

### Azure DevOps Pipeline

```yaml
trigger:
  branches:
    include:
      - prod
```

Pipeline reads `config.yaml`, builds S3 path, validates AWS access, runs quality checks. Fails release if nulls detected.

**Setup:**
1. Create AWS service connection in Azure DevOps: Project Settings → Service connections → AWS → name it `AWS-Service-Connection`
2. Add `azure-pipeline.yml` to repo
3. Create pipeline pointing to this file

## AWS Credentials

Scripts auto-detect credentials in this order:
1. `aws_profile` in config.yaml
2. AWS_PROFILE env var
3. `~/.aws/credentials` [default] section
4. Environment variables: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` + `AWS_DEFAULT_REGION`

## Files

| File | Purpose |
|---|---|
| `config.yaml` | Central configuration (single source of truth) |
| `app.py` | Streamlit UI — ouro quality checks |
| `ci_check.py` | CLI quality gate for CI/CD |
| `compare_bronze_oracle.py` | Standalone bronze vs Oracle reconciliation |
| `azure-pipeline.yml` | Azure DevOps pipeline |
| `requirements.txt` | Python dependencies |

## DRY Principle

- **S3 path:** Built from `s3_bucket`, `s3_prefix`, `s3_projeto`, `s3_tabela` in config
- **Not-null columns:** Defined once in config, used by both Streamlit and CLI
- **Date column:** Single definition for weekly timeline
- **Outlier threshold:** Shared across UI and batch checks

No hardcoding in code or pipeline.
