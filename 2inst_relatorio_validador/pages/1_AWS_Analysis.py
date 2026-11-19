"""
AWS Analysis — Verificação de nulos, timeline semanal e detecção de outliers na tabela S3/Iceberg.
"""

import streamlit as st
import pandas as pd
import numpy as np
from typing import Any
from pathlib import Path

from utils import (
    load_config, save_config, build_s3_path, resolve_aws_profile,
    is_sso_token_error, run_aws_sso_login, validate_aws_session, clear_aws_cache,
    load_parquet_s3, load_iceberg_s3, is_iceberg_table, is_container,
)

st.set_page_config(page_title="AWS Analysis", layout="wide")

cfg = load_config()


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Configurações")

    st.subheader("Caminho S3")
    col1, col2 = st.columns(2)
    with col1:
        bucket = st.text_input("Bucket", value=cfg.get("s3_bucket", "sgdai-data-lake"))
        projeto = st.text_input("Projeto", value=cfg.get("s3_projeto", "dw_2inst_ouro"))
    with col2:
        prefix = st.text_input("Prefix", value=cfg.get("s3_prefix", "ouro"))
        tabela = st.text_input("Tabela", value=cfg.get("s3_tabela", "tb_ft_meta_analitico"))

    st.caption(f"→ `{bucket}/{prefix}/{projeto}/{tabela}/`")

    aws_profile = st.text_input("AWS Profile (vazio = padrão)", value=cfg.get("aws_profile", ""))

    login_profile = aws_profile or resolve_aws_profile(cfg)
    if is_container():
        st.info("Rodando em container — autenticacao via IAM Role automatica.")
    elif login_profile:
        if st.button("Validar Sessão AWS", use_container_width=True):
            with st.spinner(f"Validando sessão AWS para {login_profile}…"):
                ok, message = validate_aws_session(login_profile)
            (st.success if ok else st.error)(message)

        if st.button("Login AWS SSO", use_container_width=True):
            with st.spinner(f"Executando aws sso login --profile {login_profile}…"):
                ok, message = run_aws_sso_login(login_profile)
            if ok:
                st.cache_data.clear()
                st.success(message)
            else:
                st.error(message)

        if st.button("Limpar Cache AWS", use_container_width=True):
            ok, message = clear_aws_cache()
            if ok:
                st.cache_data.clear()
                st.warning(message)
            else:
                st.info(message)

    date_col = st.text_input(
        "Coluna de data (para timeline semanal)",
        value=cfg.get("date_col", ""),
        placeholder="ex: dth_referencia",
    )

    not_null_cols_raw = st.text_area(
        "Colunas obrigatórias não-nulas (uma por linha)",
        value="\n".join(cfg.get("not_null_cols", [])),
        height=150,
    )
    not_null_cols = [c.strip() for c in not_null_cols_raw.splitlines() if c.strip()]

    outlier_multiplier = st.slider(
        "Multiplicador IQR (outliers)", 1.0, 3.0,
        float(cfg.get("outlier_multiplier", 1.5)), 0.1,
    )

    max_rows = st.number_input(
        "Máx. linhas a carregar (0 = todas)", min_value=0,
        value=int(cfg.get("max_rows", 500_000)), step=100_000,
    )

    if not is_container():
        if st.button("Salvar configurações"):
            save_config({
                **cfg,
                "s3_bucket": bucket, "s3_prefix": prefix,
                "s3_projeto": projeto, "s3_tabela": tabela,
                "aws_profile": aws_profile, "date_col": date_col,
                "not_null_cols": not_null_cols,
                "outlier_multiplier": outlier_multiplier,
                "max_rows": int(max_rows),
            })
            st.success("Configurações salvas.")

    run = st.button("Executar verificações", type="primary", use_container_width=True)


# ─── Check functions ──────────────────────────────────────────────────────────

def check_nulls(df: pd.DataFrame, required_cols: list[str]) -> pd.DataFrame:
    rows = []
    for col in required_cols:
        if col not in df.columns:
            rows.append({"coluna": col, "status": "AUSENTE", "qtd_nulos": None, "pct_nulos": None, "total_linhas": len(df)})
            continue
        null_count = int(df[col].isna().sum())
        null_pct = round(null_count / len(df) * 100, 2) if len(df) else 0
        rows.append({
            "coluna": col,
            "status": "OK" if null_count == 0 else "FALHOU",
            "qtd_nulos": null_count,
            "pct_nulos": null_pct,
            "total_linhas": len(df),
        })
    return pd.DataFrame(rows)


def weekly_timeline(df: pd.DataFrame, date_col: str) -> pd.DataFrame | None:
    if not date_col or date_col not in df.columns:
        return None
    s = pd.to_datetime(df[date_col], errors="coerce")
    week = s.dt.to_period("W").dt.start_time
    return week.value_counts().sort_index().rename_axis("inicio_semana").reset_index(name="qtd_linhas")


def detect_outliers(df: pd.DataFrame, k: float) -> pd.DataFrame:
    rows = []
    for col in df.select_dtypes(include=[np.number]).columns:
        s = df[col].dropna()
        if s.empty:
            continue
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        low, high = q1 - k * iqr, q3 + k * iqr
        count = int(((df[col] < low) | (df[col] > high)).sum())

        # colunas inteiras não devem exibir falsa precisão decimal
        is_int = pd.api.types.is_integer_dtype(df[col])
        def fmt(v, _is_int=is_int):
            return int(round(v)) if _is_int else round(float(v), 4)

        rows.append({
            "coluna": col,
            "tipo": str(df[col].dtype),
            "qtd_outliers": count,
            "pct_outliers": round(count / len(df) * 100, 2) if len(df) else 0,
            "q1": fmt(q1),
            "q3": fmt(q3),
            "iqr": fmt(iqr),
            "limite_inferior": fmt(low),
            "limite_superior": fmt(high),
        })
    return pd.DataFrame(rows).sort_values("qtd_outliers", ascending=False)


def style_outlier_pct(value: Any) -> str:
    if pd.isna(value):
        return ""
    v = float(value)
    if v <= 0:
        return ""
    if v >= 20:
        return "background-color: #f8b4b4"
    if v >= 5:
        return "background-color: #fbd5a5"
    return "background-color: #fde68a"


# ─── Main ─────────────────────────────────────────────────────────────────────

st.title("AWS Analysis")

effective_profile = aws_profile or resolve_aws_profile(cfg)
s3_path = build_s3_path(bucket, prefix, projeto, tabela)


@st.cache_data(ttl=300, show_spinner="Lendo dados via Iceberg metadata…")
def _load_iceberg(path, profile, nrows, db, table):
    return load_iceberg_s3(path, profile, nrows, db=db, table=table)


@st.cache_data(ttl=300, show_spinner="Lendo parquet do S3…")
def _load_raw(path, profile, nrows):
    return load_parquet_s3(path, profile, nrows)


if run:
    try:
        if is_iceberg_table(s3_path, effective_profile):
            st.session_state["aws_df"] = _load_iceberg(
                s3_path, effective_profile, int(max_rows), projeto, tabela,
            )
            st.session_state["aws_load_mode"] = "iceberg"
        else:
            st.session_state["aws_df"] = _load_raw(s3_path, effective_profile, int(max_rows))
            st.session_state["aws_load_mode"] = "raw"
        st.session_state["aws_s3_path"] = s3_path
    except Exception as e:
        if is_sso_token_error(e):
            if is_container():
                st.error("Erro de credenciais AWS. Verifique as permissoes da IAM Role do App Runner.")
            else:
                st.warning("Token SSO da AWS expirado. Limpe o cache, execute o login novamente e tente outra vez.")
                if effective_profile:
                    if st.button("Limpar Cache AWS"):
                        ok, msg = clear_aws_cache()
                        (st.warning if ok else st.info)(msg)
                    if st.button(f"Login AWS SSO ({effective_profile})", type="primary"):
                        with st.spinner(f"Executando aws sso login --profile {effective_profile}…"):
                            ok, msg = run_aws_sso_login(effective_profile)
                        if ok:
                            st.cache_data.clear()
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    st.info("Configure aws_profile na barra lateral ou AWS_PROFILE no .env antes de tentar novamente.")
        else:
            st.error(f"Falha ao carregar dados: {e}")
        st.stop()

if "aws_df" not in st.session_state:
    st.info("Configure as opções na barra lateral e clique em **Executar verificações**.")
    st.stop()

df = st.session_state["aws_df"]
_load_mode = st.session_state["aws_load_mode"]
_s3_path = st.session_state["aws_s3_path"]

mode_label = "via Iceberg catalog (snapshot atual)" if _load_mode == "iceberg" else "raw parquet"
st.success(f"Carregado **{len(df):,}** linhas × **{len(df.columns)}** colunas de `{_s3_path}` — {mode_label}")
if _load_mode == "raw":
    st.warning("Leitura raw pode incluir dados de snapshots antigos do Iceberg.")

available_columns = df.columns.tolist()
default_date_col = date_col if date_col in available_columns else None
default_not_null = [c for c in not_null_cols if c in available_columns]

with st.expander("Configurações de Análise para Esta Tabela", expanded=True):
    selected_date_col = st.selectbox(
        "Coluna de data",
        options=[""] + available_columns,
        index=(available_columns.index(default_date_col) + 1) if default_date_col else 0,
        help="Usada para a análise de timeline semanal.",
    )
    selected_not_null = st.multiselect(
        "Colunas que não podem ser nulas",
        options=available_columns,
        default=default_not_null,
    )
    st.caption("Seleções valem apenas para esta execução. Use **Salvar configurações** para persistir.")

tab_null, tab_timeline, tab_outliers, tab_schema = st.tabs([
    "Verificação de Nulos", "Timeline Semanal", "Outliers", "Schema",
])

# ── Verificação de nulos ───────────────────────────────────────────────────────
with tab_null:
    st.subheader("Verificação de Nulos")

    with st.expander("O que esta verificação faz"):
        st.markdown("""
        Varre cada coluna configurada em busca de valores `NULL` / `NaN`.

        - **OK** — nenhum nulo encontrado
        - **FALHOU** — pelo menos um nulo encontrado
        - **AUSENTE** — coluna não existe na tabela (desvio de schema)

        **Por que importa:** Nulo em colunas-chave (`id_processo`, `coddoc`, métricas obrigatórias)
        indica ETL quebrado, join com falha ou problema na extração da fonte.
        Um nulo em identificador de processo torna aquele registro não rastreável nos relatórios.
        """)

    if not selected_not_null:
        st.warning("Nenhuma coluna obrigatória selecionada acima.")
    else:
        result = check_nulls(df, selected_not_null)
        fail = result[result["status"] != "OK"]
        ok = result[result["status"] == "OK"]

        c1, c2, c3 = st.columns(3)
        c1.metric("Colunas verificadas", len(result))
        c2.metric("Aprovadas", len(ok), delta_color="normal")
        c3.metric("Reprovadas / Ausentes", len(fail), delta_color="inverse")

        if not fail.empty:
            st.error("Algumas colunas reprovaram na verificação de nulos:")
            st.dataframe(
                fail.style.map(
                    lambda v: "background-color: #ffcccc" if v in ("FALHOU", "AUSENTE") else "",
                    subset=["status"],
                ),
                use_container_width=True,
            )
        else:
            st.success("Todas as colunas obrigatórias estão sem nulos.")

        with st.expander("Resultados completos"):
            st.dataframe(result, use_container_width=True)

# ── Timeline semanal ───────────────────────────────────────────────────────────
with tab_timeline:
    st.subheader("Contagem de Linhas por Semana")

    with st.expander("O que esta verificação faz"):
        st.markdown("""
        Agrupa linhas por semana calendário usando a coluna de data e plota a contagem por semana.
        Sinaliza semanas abaixo de **média − 2σ** como anormalmente baixas.

        **Por que importa:** Queda brusca no volume semanal indica:
        - Falha no pipeline ou carga parcial para aquele período
        - Indisponibilidade do sistema de origem na janela de extração
        - Retenção ou exclusão de dados

        Pipelines estáveis devem ter volume semanal aproximadamente consistente.
        """)

    tl = weekly_timeline(df, selected_date_col)
    if tl is None:
        st.warning("Selecione uma coluna de data acima para ativar a timeline.")
    else:
        st.line_chart(tl.set_index("inicio_semana")["qtd_linhas"])
        mean_c = tl["qtd_linhas"].mean()
        std_c = tl["qtd_linhas"].std()
        low_weeks = tl[tl["qtd_linhas"] < mean_c - 2 * std_c]
        if not low_weeks.empty:
            st.warning(f"**{len(low_weeks)} semana(s)** com contagem de linhas anormalmente baixa (< média − 2σ):")
            st.dataframe(low_weeks, use_container_width=True)
        else:
            st.success("Nenhuma semana com contagem anormalmente baixa.")
        st.dataframe(tl, use_container_width=True)

# ── Outliers ───────────────────────────────────────────────────────────────────
with tab_outliers:
    st.subheader(f"Detecção de Outliers (IQR × {outlier_multiplier})")

    with st.expander("O que esta verificação faz"):
        st.markdown(f"""
        Usa o **método IQR fence** para sinalizar outliers estatísticos em todas as colunas numéricas.

        ```
        limite inferior = Q1 − {outlier_multiplier} × IQR
        limite superior = Q3 + {outlier_multiplier} × IQR
        ```

        **Colunas inteiras** — Q1, Q3 e limites exibidos como inteiros (sem falsa precisão decimal).
        Colunas float exibem 4 casas decimais.

        **Escala de cores:** amarelo = <5%, laranja = 5–20%, vermelho = ≥20% de outliers.

        **Por que importa:**
        - Erros de codificação produzem valores-sentinela extremos (ex: 9999)
        - Bugs de cálculo no ETL geram valores de métricas irreais
        - Outliers em `qt_acervo` ou numeradores de meta distorcem os relatórios agregados
        """)

    out_df = detect_outliers(df, outlier_multiplier)
    if out_df.empty:
        st.info("Nenhuma coluna numérica encontrada.")
    else:
        flagged = out_df[out_df["qtd_outliers"] > 0]
        st.metric("Colunas com outliers", len(flagged))
        st.dataframe(
            out_df.style.map(style_outlier_pct, subset=["pct_outliers"]),
            use_container_width=True,
        )

# ── Schema ─────────────────────────────────────────────────────────────────────
with tab_schema:
    st.subheader("Schema e Amostra")

    with st.expander("O que esta verificação faz"):
        st.markdown("""
        Exibe nomes de colunas, tipos de dados e amostra de linhas.

        **Alerta automático:** colunas com nomes que combinam com `qt_|id_|nr_|cd_` armazenadas como `float64`
        — comum quando o pandas lê colunas inteiras com nulos (inteiro nullable vira float).
        Devem ser convertidas para `Int64` (inteiro nullable) no ETL.

        Use a página **Source vs AWS** para comparar este schema com o banco de origem.
        """)

    dtypes_df = pd.DataFrame({"coluna": df.columns, "tipo": df.dtypes.values.astype(str)})

    int_as_float = dtypes_df[
        dtypes_df["tipo"].isin(["float64", "float32"])
        & dtypes_df["coluna"].str.contains("qt_|id_|nr_|cd_", regex=True)
    ]
    if not int_as_float.empty:
        st.warning(
            f"**{len(int_as_float)} coluna(s)** parecem inteiros mas estão armazenadas como float "
            "(pandas lê inteiro-com-nulos como float). Considere converter para `Int64` no ETL."
        )
        st.dataframe(int_as_float, use_container_width=True)

    st.dataframe(dtypes_df, use_container_width=True)
    st.subheader("Amostra de linhas (primeiras 100)")
    st.dataframe(df.head(100), use_container_width=True)
