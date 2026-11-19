"""
Origem vs AWS — Reconciliação de contagem de linhas e schema.
Suporta Oracle, MySQL e PostgreSQL como bancos de origem.
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime

import db_connector as dbc
from utils import (
    load_config, resolve_aws_profile, is_container,
    is_sso_token_error, run_aws_sso_login, validate_aws_session, clear_aws_cache,
    load_parquet_s3, load_iceberg_s3, is_iceberg_table,
)
from secrets_manager import get_oracle_config

st.set_page_config(page_title="Origem vs AWS", layout="wide")

cfg = load_config()

# ─── Sidebar — conexões ────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Conexões")

    # ── Banco de origem ────────────────────────────────────────────────────────
    st.subheader("Banco de Dados de Origem")
    db_type = st.selectbox("Tipo de banco", dbc.DB_TYPES)

    available, driver_err = dbc.is_available(db_type)
    if not available:
        st.error(driver_err)

    # pre-fill Oracle from secrets_manager (env vars or Secrets Manager)
    _ora_sec = get_oracle_config() if db_type == "Oracle" else {}
    _pre_host = _ora_sec.get("host", "") if db_type == "Oracle" else ""
    _pre_port = int(_ora_sec.get("port", dbc.default_port(db_type))) if db_type == "Oracle" else dbc.default_port(db_type)
    _pre_svc  = _ora_sec.get("service_name", "") if db_type == "Oracle" else ""
    _pre_user = _ora_sec.get("user", "") if db_type == "Oracle" else ""
    _pre_env  = "ORACLE_PASSWORD" if db_type == "Oracle" else "DB_PASSWORD"

    db_host = st.text_input("Host", value=_pre_host)
    db_port = st.number_input("Porta", value=_pre_port, step=1)
    db_label = {"Oracle": "Nome do serviço", "MySQL": "Banco de dados", "PostgreSQL": "Banco de dados"}.get(db_type, "Banco de dados")
    db_database = st.text_input(db_label, value=_pre_svc)
    db_user = st.text_input("Usuário", value=_pre_user)
    db_pwd_env = st.text_input("Variável de ambiente da senha", value=_pre_env)

    # ── AWS ────────────────────────────────────────────────────────────────────
    st.subheader("AWS")
    aws_profile = st.text_input(
        "AWS Profile", value=cfg.get("aws_profile", "") or resolve_aws_profile(cfg)
    )
    s3_bucket = st.text_input("Bucket S3", value=cfg.get("s3_bucket", "sgdai-data-lake"))

    if is_container():
        st.info("Rodando em container — autenticacao via IAM Role automatica.")
    elif aws_profile:
        if st.button("Validar Sessão AWS", use_container_width=True):
            with st.spinner(f"Validando {aws_profile}…"):
                ok, msg = validate_aws_session(aws_profile)
            (st.success if ok else st.error)(msg)
        if st.button("Login AWS SSO", use_container_width=True):
            with st.spinner(f"Executando aws sso login --profile {aws_profile}…"):
                ok, msg = run_aws_sso_login(aws_profile)
            if ok:
                st.cache_data.clear()
                st.success(msg)
            else:
                st.error(msg)
        if st.button("Limpar Cache AWS", use_container_width=True):
            ok, msg = clear_aws_cache()
            (st.warning if ok else st.info)(msg)
            if ok:
                st.cache_data.clear()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_db_conn():
    if db_type == "Oracle" and is_container():
        from secrets_manager import get_oracle_config
        ora_cfg = get_oracle_config()
        password = ora_cfg.get("password", "")
    else:
        password = os.environ.get(db_pwd_env, "")
    if not password:
        st.error(f"Senha nao encontrada. Configure `{db_pwd_env}` no `.env` ou no AWS Secrets Manager.")
        st.stop()
    if not available:
        st.error(driver_err)
        st.stop()
    try:
        return dbc.connect(db_type, db_host, int(db_port), db_database, db_user, password)
    except Exception as e:
        st.error(f"Falha na conexão com {db_type}: {e}")
        st.stop()


@st.cache_data(ttl=300, show_spinner="Carregando dados via Iceberg metadata…")
def _load_aws_iceberg(bucket: str, rel_path: str, profile: str, max_rows: int) -> pd.DataFrame:
    s3_path = f"s3://{bucket}/{rel_path.strip('/')}/"
    parts = rel_path.strip("/").split("/")
    db = parts[-2] if len(parts) >= 2 else parts[0]
    table = parts[-1]
    return load_iceberg_s3(s3_path, profile, max_rows, db=db, table=table)


@st.cache_data(ttl=300, show_spinner="Carregando parquet do S3 (raw)…")
def _load_aws_raw(bucket: str, rel_path: str, profile: str, max_rows: int) -> pd.DataFrame:
    return load_parquet_s3(f"s3://{bucket}/{rel_path.strip('/')}", profile, max_rows)


def aws_count_filtered(df: pd.DataFrame, date_col: str, period_type: str, period_val: str) -> int:
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


# ─── Principal ────────────────────────────────────────────────────────────────

st.title("Origem vs AWS")

with st.expander("Como esta página funciona"):
    st.markdown("""
    Compara contagem de linhas e schemas entre uma tabela do banco de origem e uma tabela S3/Iceberg.

    **Bancos de origem suportados:** Oracle, MySQL, PostgreSQL

    **Reconciliação de contagem de linhas**
    - Origem: `SELECT COUNT(*) FROM tabela [WHERE filtro de período]`
    - AWS: carrega parquet e filtra em memória pelo mesmo período

    **Diff de schema** — nomes de colunas da origem vs nomes no parquet

    **Formato tabela Oracle:** `SCHEMA.TABELA` ex: `TJ01.MOVIMENTORELATOR`
    **MySQL/PostgreSQL:** `schema.tabela` ou somente `tabela`
    **Caminho AWS:** relativo ao bucket ex: `bronze/ejud/movimentorelator`
    """)

# ─── Seleção rápida do config ─────────────────────────────────────────────────

bronze_tables = cfg.get("bronze_tables", [])
if bronze_tables:
    with st.expander("Seleção rápida das bronze_tables configuradas"):
        options = {f"{t['oracle_table']} ↔ {t['s3_path']}": t for t in bronze_tables}
        choice = st.selectbox("Escolha um par para pré-preencher", [""] + list(options.keys()))
        selected = options[choice] if choice else None
else:
    selected = None

# ─── Par de tabelas ───────────────────────────────────────────────────────────

st.subheader("Par de tabelas")

col_src, col_aws = st.columns(2)
with col_src:
    src_table = st.text_input(
        f"Tabela {db_type}",
        value=selected["oracle_table"] if selected else "",
        placeholder="ex: TJ01.MOVIMENTORELATOR  ou  schema.tabela",
    )
with col_aws:
    aws_rel_path = st.text_input(
        "Caminho S3 na AWS (relativo ao bucket)",
        value=selected["s3_path"] if selected else "",
        placeholder="ex: bronze/ejud/movimentorelator",
    )

# ─── Filtro de período ────────────────────────────────────────────────────────

st.subheader("Filtro de período (opcional — recomendado para tabelas grandes)")

col_dc, col_pt, col_pv, col_thr = st.columns([2, 1, 2, 1])
with col_dc:
    date_col = st.text_input(
        "Coluna de data (mesmo nome na origem e na AWS)",
        value=cfg.get("date_col", ""),
        placeholder="ex: dthrmov",
    )
with col_pt:
    period_type = st.selectbox("Granularidade", ["None", "Month", "Year"])
with col_pv:
    now = datetime.now()
    if period_type == "Month":
        period_val = st.text_input("Período (AAAA-MM)", value=f"{now.year}-{now.month:02d}")
    elif period_type == "Year":
        period_val = st.text_input("Período (AAAA)", value=str(now.year))
    else:
        period_val = ""
        st.text_input("Período", value="—", disabled=True)
with col_thr:
    threshold_pct = st.number_input(
        "Limite para FAIL (%)", min_value=0.0, value=0.0, step=0.1,
        help="Diff_pct acima deste valor → FAIL. 0 = qualquer diferença é FAIL.",
    )

max_rows_aws = st.number_input(
    "Máx. linhas a carregar da AWS (0 = todas)",
    min_value=0, value=int(cfg.get("max_rows", 500_000)), step=100_000,
)

run = st.button("Comparar", type="primary")

if run:
    # ─── Validação ────────────────────────────────────────────────────────────
    errors = []
    if not src_table:
        errors.append(f"Tabela {db_type} obrigatória.")
    if not aws_rel_path:
        errors.append("Caminho S3 na AWS obrigatório.")
    if not db_host or not db_database or not db_user:
        errors.append("Configurações de conexão com o banco de origem incompletas (verifique a barra lateral).")
    for e in errors:
        st.error(e)
    if errors:
        st.stop()

    # ─── Carregamento AWS ─────────────────────────────────────────────────────
    with st.spinner("Carregando dados da AWS…"):
        try:
            _aws_path = f"s3://{s3_bucket}/{aws_rel_path.strip('/')}/"
            if is_iceberg_table(_aws_path, aws_profile):
                st.session_state["srcaws_df"] = _load_aws_iceberg(s3_bucket, aws_rel_path, aws_profile, int(max_rows_aws))
                st.session_state["srcaws_load_mode"] = "iceberg"
            else:
                st.session_state["srcaws_df"] = _load_aws_raw(s3_bucket, aws_rel_path, aws_profile, int(max_rows_aws))
                st.session_state["srcaws_load_mode"] = "raw"
            st.session_state["srcaws_s3_display"] = f"s3://{s3_bucket}/{aws_rel_path.strip('/')}/"
        except Exception as e:
            if is_sso_token_error(e):
                st.warning("Token SSO da AWS expirado. Use o botão Login AWS SSO na barra lateral.")
            else:
                st.error(f"Falha ao carregar dados da AWS: {e}")
            st.stop()

    if st.session_state["srcaws_df"].empty:
        st.error(f"Nenhum arquivo parquet em `{st.session_state['srcaws_s3_display']}`")
        st.session_state.pop("srcaws_df", None)
        st.stop()

    # ─── Conexão com banco de origem + queries ────────────────────────────────
    with st.spinner(f"Conectando ao {db_type}…"):
        conn = get_db_conn()

    _df_aws_tmp = st.session_state["srcaws_df"]
    with st.spinner(f"Contando linhas no {db_type}…"):
        try:
            st.session_state["srcaws_src_cnt"] = dbc.count_rows(
                conn, db_type, src_table, date_col, period_type, period_val,
            )
        except Exception as e:
            st.session_state["srcaws_src_cnt"] = None
            st.session_state["srcaws_src_cnt_err"] = str(e)

    st.session_state["srcaws_aws_cnt"] = aws_count_filtered(
        _df_aws_tmp, date_col, period_type, period_val,
    )

    with st.spinner(f"Obtendo schema do {db_type}…"):
        try:
            st.session_state["srcaws_src_cols"] = dbc.get_columns(conn, db_type, src_table)
        except Exception as e:
            st.session_state["srcaws_src_cols"] = []
            st.session_state["srcaws_src_cols_err"] = str(e)

    conn.close()

    st.session_state["srcaws_db_type"] = db_type
    st.session_state["srcaws_period_label"] = (
        f" [{period_type}: {period_val}]" if period_type != "None" and period_val else ""
    )
    st.session_state["srcaws_threshold_pct"] = threshold_pct
    st.session_state["srcaws_max_rows"] = int(max_rows_aws)

if "srcaws_df" not in st.session_state:
    st.info("Configure as opções e clique em **Comparar**.")
    st.stop()

df_aws = st.session_state["srcaws_df"]
_load_mode = st.session_state["srcaws_load_mode"]
_s3_display = st.session_state["srcaws_s3_display"]
_db_type = st.session_state.get("srcaws_db_type", db_type)
_period_label = st.session_state.get("srcaws_period_label", "")
_threshold_pct = st.session_state.get("srcaws_threshold_pct", 0.0)
_max_rows_aws = st.session_state.get("srcaws_max_rows", 0)

_mode_label = "via Iceberg (snapshot atual)" if _load_mode == "iceberg" else "raw parquet"
st.success(
    f"AWS: **{len(df_aws):,}** linhas × **{len(df_aws.columns)}** colunas "
    f"de `{_s3_display}` — {_mode_label}"
)
if _load_mode == "raw":
    st.warning("Leitura raw pode incluir dados de snapshots antigos do Iceberg.")

# ─── Abas ─────────────────────────────────────────────────────────────────────

tab_count, tab_schema = st.tabs(["Contagem de Linhas", "Diferença de Schema"])

# ── Contagem de linhas ─────────────────────────────────────────────────────────
with tab_count:
    st.subheader("Reconciliação de Contagem de Linhas")

    src_cnt = st.session_state.get("srcaws_src_cnt")
    aws_cnt = st.session_state.get("srcaws_aws_cnt", 0)

    if "srcaws_src_cnt_err" in st.session_state and src_cnt is None:
        st.error(st.session_state["srcaws_src_cnt_err"])

    if src_cnt is None:
        st.error(f"Não foi possível contar linhas no {_db_type}.")
    else:
        diff = aws_cnt - src_cnt
        diff_pct = round(abs(diff) / src_cnt * 100, 4) if src_cnt else 0.0

        if diff == 0:
            status = "✅ OK"
        elif diff_pct <= _threshold_pct:
            status = "⚠️ WARN"
        else:
            status = "❌ FAIL"

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric(f"{_db_type}{_period_label}", f"{src_cnt:,}")
        c2.metric(f"AWS{_period_label}", f"{aws_cnt:,}")
        c3.metric("Diferença (AWS − Origem)", f"{diff:+,}")
        c4.metric("Diferença %", f"{diff_pct:.4f}%")
        c5.metric("Status", status)

        if _max_rows_aws > 0 and len(df_aws) >= _max_rows_aws:
            st.warning(
                f"Contagem AWS atingiu o limite máximo de linhas ({_max_rows_aws:,}). "
                "A contagem real pode ser maior. Defina máx. linhas = 0 para carregar todas."
            )

        if diff != 0:
            st.markdown("**Possíveis causas:**")
            if diff > 0:
                st.markdown("- AWS tem mais linhas — duplicatas introduzidas no ETL")
                st.markdown("- Dados da origem excluídos/arquivados após a extração")
            else:
                st.markdown("- AWS tem menos linhas — carga parcial ou falha na extração")
                st.markdown("- Origem tem linhas inseridas após a última execução do pipeline")

# ── Diferença de schema ────────────────────────────────────────────────────────
with tab_schema:
    st.subheader("Comparação de Schema")

    src_cols = st.session_state.get("srcaws_src_cols", [])
    if "srcaws_src_cols_err" in st.session_state and not src_cols:
        st.error(st.session_state["srcaws_src_cols_err"])

    aws_cols = [c.upper() for c in df_aws.columns.tolist()]

    src_set, aws_set = set(src_cols), set(aws_cols)
    only_src = sorted(src_set - aws_set)
    only_aws = sorted(aws_set - src_set)
    shared = sorted(src_set & aws_set)

    c1, c2, c3 = st.columns(3)
    c1.metric(f"Colunas {_db_type}", len(src_cols))
    c2.metric("Colunas AWS", len(aws_cols))
    c3.metric("Em comum", len(shared))

    if only_src:
        st.error(f"**{len(only_src)} coluna(s) só na origem {_db_type}** — ausentes na AWS:")
        st.dataframe(pd.DataFrame({"coluna": only_src}), use_container_width=True)
    else:
        st.success("Nenhuma coluna da origem ausente na AWS.")

    if only_aws:
        st.warning(f"**{len(only_aws)} coluna(s) só na AWS** — não existem na origem:")
        st.dataframe(pd.DataFrame({"coluna": only_aws}), use_container_width=True)

    with st.expander(f"Colunas em comum ({len(shared)})"):
        st.dataframe(pd.DataFrame({"coluna": shared}), use_container_width=True)
