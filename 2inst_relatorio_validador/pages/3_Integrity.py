"""
Integridade de Dados — Verificações de fase 3: duplicatas, domínio de valores,
ordenação de datas, limites numéricos e tendência de volume.
Todas as verificações rodam sobre os dados parquet no S3/Iceberg.
"""

import streamlit as st
import pandas as pd
import numpy as np
from typing import Any

from utils import (
    load_config, resolve_aws_profile, is_container,
    is_sso_token_error, run_aws_sso_login, validate_aws_session, clear_aws_cache,
    load_parquet_s3, load_iceberg_s3, is_iceberg_table,
)

st.set_page_config(page_title="Integridade de Dados", layout="wide")

cfg = load_config()

# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Configurações AWS")

    col1, col2 = st.columns(2)
    with col1:
        bucket  = st.text_input("Bucket",  value=cfg.get("s3_bucket",  "sgdai-data-lake"))
        projeto = st.text_input("Projeto", value=cfg.get("s3_projeto", "dw_2inst_ouro"))
    with col2:
        prefix = st.text_input("Prefix", value=cfg.get("s3_prefix", "ouro"))
        tabela = st.text_input("Tabela", value=cfg.get("s3_tabela",  "tb_ft_meta_analitico"))

    st.caption(f"→ `{bucket}/{prefix}/{projeto}/{tabela}/`")

    aws_profile = st.text_input(
        "AWS Profile", value=cfg.get("aws_profile", "") or resolve_aws_profile(cfg)
    )

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

    max_rows = st.number_input(
        "Máx. linhas a carregar (0 = todas)", min_value=0,
        value=int(cfg.get("max_rows", 500_000)), step=100_000,
    )

    run = st.button("Carregar dados", type="primary", use_container_width=True)


# ─── Carregamento de dados ────────────────────────────────────────────────────

st.title("Integridade de Dados")

with st.expander("Verificações de integridade disponíveis"):
    st.markdown("""
    | Verificação | O que detecta |
    |-------------|--------------|
    | **Duplicatas** | Linhas com mesma chave — dedup não aplicado no ETL |
    | **Domínio de valores** | Valores fora do conjunto esperado — erros de codificação, códigos desconhecidos |
    | **Ordenação de datas** | Linhas onde data início > data fim — lógica de datas quebrada no pipeline |
    | **Limites numéricos** | Valores abaixo do mínimo ou acima do máximo — sentinelas, overflow, bugs de ETL |
    | **Tendência de volume** | Variação % de contagem período a período — cargas parciais, gaps no pipeline |

    Todas as verificações rodam sobre os dados parquet carregados do S3/Iceberg.
    Configure a tabela na barra lateral e clique em **Carregar dados**.
    """)


@st.cache_data(ttl=300, show_spinner="Lendo dados via Iceberg metadata…")
def _load_iceberg(bucket, prefix, projeto, tabela, profile, nrows):
    parts = [p for p in [bucket, prefix, projeto, tabela] if p]
    path = "s3://" + "/".join(parts) + "/"
    return load_iceberg_s3(path, profile, nrows, db=projeto, table=tabela)


@st.cache_data(ttl=300, show_spinner="Lendo parquet do S3 (raw)…")
def _load_raw(bucket, prefix, projeto, tabela, profile, nrows):
    parts = [p for p in [bucket, prefix, projeto, tabela] if p]
    path = "s3://" + "/".join(parts) + "/"
    return load_parquet_s3(path, profile, nrows)


if run:
    st.session_state.pop("dup_result", None)
    try:
        s3_path = "s3://" + "/".join(p for p in [bucket, prefix, projeto, tabela] if p) + "/"
        if is_iceberg_table(s3_path, aws_profile):
            st.session_state["integrity_df"] = _load_iceberg(bucket, prefix, projeto, tabela, aws_profile, int(max_rows))
            st.session_state["_load_mode"] = "iceberg"
        else:
            st.session_state["integrity_df"] = _load_raw(bucket, prefix, projeto, tabela, aws_profile, int(max_rows))
            st.session_state["_load_mode"] = "raw"
    except Exception as e:
        if is_sso_token_error(e):
            st.warning("Token SSO da AWS expirado. Use o botão Login AWS SSO na barra lateral.")
        else:
            st.error(f"Falha ao carregar dados: {e}")
        st.stop()

if "integrity_df" not in st.session_state:
    st.info("Configure o caminho S3 na barra lateral e clique em **Carregar dados**.")
    st.stop()

df = st.session_state["integrity_df"]

if df.empty:
    st.error("Nenhum dado carregado.")
    st.stop()

load_mode = st.session_state.get("_load_mode", "raw")
mode_label = "via Iceberg catalog (snapshot atual)" if load_mode == "iceberg" else "raw parquet (sem filtro de snapshot)"
st.success(f"Carregado **{len(df):,}** linhas × **{len(df.columns)}** colunas — {mode_label}")
if load_mode == "raw":
    st.warning("Leitura raw de parquet pode incluir dados de snapshots antigos do Iceberg. "
               "Instale `pyiceberg` para leitura correta: `pip install pyiceberg[glue]`")

cols = df.columns.tolist()
num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
date_like_cols = [c for c in cols if any(k in c.lower() for k in ["dth", "dat", "data", "date", "dt_", "_dt"])]

# ─── Abas ─────────────────────────────────────────────────────────────────────

tab_dup, tab_domain, tab_dates, tab_bounds, tab_trend = st.tabs([
    "Duplicatas", "Domínio de Valores", "Ordenação de Datas", "Limites Numéricos", "Tendência de Volume",
])


# ── 1. Duplicatas ──────────────────────────────────────────────────────────────
with tab_dup:
    st.subheader("Detecção de Chaves Duplicadas")

    with st.expander("Por que isso importa"):
        st.markdown("""
        Chaves primárias duplicadas fazem um processo/caso aparecer múltiplas vezes nas agregações.
        Causa comum: re-execução do ETL sem deduplicação, joins incorretos multiplicando linhas.

        **Exemplo:** `coddoc` duplicado em `tb_ft_meta_analitico` → toda contagem de meta inflada.
        """)

    st.markdown("**Colunas-chave** — desmarque as que **não** fazem parte da chave primária:")

    if "dup_col_state" not in st.session_state or st.session_state.get("_dup_cols_source") != cols:
        st.session_state["dup_col_state"] = {c: True for c in cols}
        st.session_state["_dup_cols_source"] = cols

    col_left, col_right = st.columns(2)
    half = (len(cols) + 1) // 2
    for i, c in enumerate(cols):
        target = col_left if i < half else col_right
        with target:
            st.session_state["dup_col_state"][c] = st.checkbox(
                c, value=st.session_state["dup_col_state"].get(c, True), key=f"dup_chk_{c}",
            )

    key_cols = [c for c in cols if st.session_state["dup_col_state"].get(c, False)]

    if not key_cols:
        st.warning("Selecione ao menos uma coluna-chave.")
    else:
        st.caption(f"Chave selecionada: `{', '.join(key_cols)}`")
        if st.button("Verificar duplicatas", key="btn_dup"):
            with st.spinner("Verificando duplicatas…"):
                check_df = df[key_cols].astype(str)
                dup_mask = check_df.duplicated(keep=False)
                dup_count = int(dup_mask.sum())
                dup_pct = round(dup_count / len(df) * 100, 4) if len(df) else 0
                dup_summary = (
                    check_df[dup_mask]
                    .groupby(key_cols)
                    .size()
                    .reset_index(name="ocorrencias")
                    .sort_values("ocorrencias", ascending=False)
                ) if dup_count > 0 else None

            st.session_state["dup_result"] = {
                "key_cols": list(key_cols),
                "dup_count": dup_count,
                "dup_pct": dup_pct,
                "dup_summary": dup_summary,
                "total": len(df),
            }

        result = st.session_state.get("dup_result")
        if result and result["key_cols"] == key_cols:
            st.caption(f"Verificado com colunas: `{', '.join(result['key_cols'])}`")
            c1, c2, c3 = st.columns(3)
            c1.metric("Total de linhas", f"{result['total']:,}")
            c2.metric("Linhas duplicadas", f"{result['dup_count']:,}")
            c3.metric("% Duplicadas", f"{result['dup_pct']:.4f}%",
                      delta=f"{'FAIL' if result['dup_count'] > 0 else 'OK'}",
                      delta_color="inverse" if result['dup_count'] > 0 else "normal")

            if result["dup_count"] > 0:
                st.error(f"Encontradas {result['dup_count']:,} linhas com chave(s) duplicada(s).")
                st.markdown("**Chaves mais duplicadas:**")
                st.dataframe(result["dup_summary"].head(50), use_container_width=True)
            else:
                st.success(f"Nenhuma duplicata encontrada na chave: {', '.join(result['key_cols'])}")
        elif result and result["key_cols"] != key_cols:
            st.info("Colunas alteradas — clique em **Verificar duplicatas** para atualizar.")


# ── 2. Domínio de valores ──────────────────────────────────────────────────────
with tab_domain:
    st.subheader("Validação de Domínio de Valores")

    with st.expander("Por que isso importa"):
        st.markdown("""
        Colunas como `situacao`, `cd_meta`, `tp_orgao` devem conter apenas códigos conhecidos.
        Valores desconhecidos indicam:
        - Novos códigos adicionados ao sistema de origem ainda não mapeados no ETL
        - Corrupção de dados ou erros de codificação
        - Tabela errada usada no join do pipeline

        **Exemplo:** `situacao` deveria estar em `{0, 1, 2, 3}` — valor `99` → mapeamento quebrado.
        """)

    domain_col = st.selectbox("Coluna a verificar", [""] + cols, key="domain_col")
    expected_raw = st.text_area(
        "Valores esperados (um por linha ou separados por vírgula)",
        placeholder="ex:\n0\n1\n2\n3",
        height=120,
        key="domain_vals",
    )

    if domain_col and expected_raw:
        if st.button("Verificar domínio", key="btn_domain"):
            raw_vals = [v.strip() for v in expected_raw.replace(",", "\n").splitlines() if v.strip()]
            col_dtype = df[domain_col].dtype

            try:
                if pd.api.types.is_integer_dtype(col_dtype):
                    expected = set(int(v) for v in raw_vals)
                elif pd.api.types.is_float_dtype(col_dtype):
                    expected = set(float(v) for v in raw_vals)
                else:
                    expected = set(raw_vals)
            except ValueError:
                expected = set(raw_vals)

            with st.spinner("Verificando valores…"):
                actual_vals = df[domain_col].dropna().unique()
                unexpected = set(str(v) for v in actual_vals) - set(str(v) for v in expected)
                bad_mask = df[domain_col].astype(str).isin(unexpected)
                bad_count = int(bad_mask.sum())
                bad_pct = round(bad_count / len(df) * 100, 4) if len(df) else 0

            c1, c2, c3 = st.columns(3)
            c1.metric("Valores esperados", len(expected))
            c2.metric("Linhas com valor inesperado", f"{bad_count:,}")
            c3.metric("% Inesperado", f"{bad_pct:.4f}%")

            if bad_count > 0:
                st.error(f"Encontrados **{len(unexpected)}** valor(es) inesperado(s) em `{domain_col}`:")
                bad_summary = (
                    df[bad_mask][domain_col]
                    .value_counts()
                    .reset_index()
                    .rename(columns={"index": domain_col, domain_col: "contagem"})
                )
                st.dataframe(bad_summary, use_container_width=True)
            else:
                st.success(f"Todos os valores em `{domain_col}` estão dentro do domínio esperado.")

            with st.expander("Todos os valores distintos encontrados"):
                val_counts = df[domain_col].value_counts().reset_index()
                val_counts.columns = [domain_col, "contagem"]
                st.dataframe(val_counts, use_container_width=True)
    elif domain_col and not expected_raw:
        st.info("Informe os valores esperados para executar a verificação.")

        with st.spinner("Obtendo valores distintos…"):
            val_counts = df[domain_col].value_counts().head(100).reset_index()
            val_counts.columns = [domain_col, "contagem"]
        st.markdown("**Valores distintos na coluna (top 100):**")
        st.dataframe(val_counts, use_container_width=True)


# ── 3. Ordenação de datas ──────────────────────────────────────────────────────
with tab_dates:
    st.subheader("Validação de Ordenação de Datas")

    with st.expander("Por que isso importa"):
        st.markdown("""
        Violações de lógica de datas como `dth_baixa < dth_distribuicao` (processo baixado antes de ser distribuído)
        indicam dados corrompidos na origem ou transformações ETL quebradas.

        Pares comuns a verificar:
        - `dth_distribuicao` ≤ `dth_baixa`
        - `dth_inicio` ≤ `dth_fim`
        - `dth_autuacao` ≤ `dth_julgamento`
        """)

    st.markdown("**Adicione pares de colunas de data para verificar (início deve ser ≤ fim):**")

    n_pairs = st.number_input("Número de pares de data", min_value=1, max_value=10, value=1, step=1)

    pairs = []
    for i in range(int(n_pairs)):
        c1, c2 = st.columns(2)
        with c1:
            start_col = st.selectbox(
                f"Par {i+1}: Coluna de início",
                [""] + date_like_cols + cols,
                key=f"date_start_{i}",
            )
        with c2:
            end_col = st.selectbox(
                f"Par {i+1}: Coluna de fim",
                [""] + date_like_cols + cols,
                key=f"date_end_{i}",
            )
        if start_col and end_col and start_col != end_col:
            pairs.append((start_col, end_col))

    if pairs and st.button("Verificar ordenação de datas", key="btn_dates"):
        results = []
        for start_col, end_col in pairs:
            with st.spinner(f"Verificando {start_col} ≤ {end_col}…"):
                s = pd.to_datetime(df[start_col], errors="coerce")
                e = pd.to_datetime(df[end_col], errors="coerce")
                both_valid = s.notna() & e.notna()
                violation_mask = both_valid & (s > e)
                vcount = int(violation_mask.sum())
                vpct = round(vcount / int(both_valid.sum()) * 100, 4) if both_valid.sum() else 0
                results.append({
                    "coluna_inicio": start_col,
                    "coluna_fim": end_col,
                    "linhas_verificadas": int(both_valid.sum()),
                    "violacoes": vcount,
                    "pct_violacoes": vpct,
                    "status": "✅ OK" if vcount == 0 else "❌ FAIL",
                })

        results_df = pd.DataFrame(results)
        st.dataframe(results_df, use_container_width=True)

        for (start_col, end_col), row in zip(pairs, results):
            if row["violacoes"] > 0:
                st.error(f"`{start_col}` > `{end_col}` em {row['violacoes']:,} linhas. Amostra:")
                s2 = pd.to_datetime(df[start_col], errors="coerce")
                e2 = pd.to_datetime(df[end_col], errors="coerce")
                mask = s2.notna() & e2.notna() & (s2 > e2)
                st.dataframe(df[mask][[start_col, end_col]].head(20), use_container_width=True)


# ── 4. Limites numéricos ───────────────────────────────────────────────────────
with tab_bounds:
    st.subheader("Validação de Limites Numéricos")

    with st.expander("Por que isso importa"):
        st.markdown("""
        Colunas de contagem (`qt_acervo`, `qt_decididos`) nunca devem ser negativas.
        Colunas de taxa/percentual devem estar entre 0 e 1 (ou 0–100).
        Valores fora dos limites esperados indicam:
        - Overflow em operações aritméticas
        - Valores-sentinela não filtrados (ex: -1, 9999)
        - Join incorreto produzindo valores multiplicados ou subtraídos

        Deixe mínimo ou máximo em branco para ignorar aquele limite.
        """)

    n_rules = st.number_input("Número de regras", min_value=1, max_value=15, value=3, step=1)

    suggested = [c for c in num_cols if any(p in c.lower() for p in ["qt_", "nr_", "vl_", "pc_"])]

    rules = []
    for i in range(int(n_rules)):
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1:
            rcol = st.selectbox(
                f"Regra {i+1}: Coluna",
                [""] + num_cols,
                index=min(i + 1, len(num_cols)),
                key=f"bound_col_{i}",
            )
        with c2:
            rmin = st.text_input("Mín", value="0" if i < len(suggested) else "", key=f"bound_min_{i}",
                                  placeholder="(nenhum)")
        with c3:
            rmax = st.text_input("Máx", value="", key=f"bound_max_{i}", placeholder="(nenhum)")
        if rcol:
            try:
                min_val = float(rmin) if rmin.strip() else None
                max_val = float(rmax) if rmax.strip() else None
            except ValueError:
                min_val = max_val = None
            rules.append((rcol, min_val, max_val))

    valid_rules = [(c, mn, mx) for c, mn, mx in rules if c and (mn is not None or mx is not None)]

    if valid_rules and st.button("Verificar limites", key="btn_bounds"):
        bound_results = []
        for col, min_val, max_val in valid_rules:
            s = df[col].dropna()
            below = int((s < min_val).sum()) if min_val is not None else 0
            above = int((s > max_val).sum()) if max_val is not None else 0
            total_bad = below + above
            pct = round(total_bad / len(s) * 100, 4) if len(s) else 0
            bound_results.append({
                "coluna": col,
                "limite_min": min_val if min_val is not None else "—",
                "limite_max": max_val if max_val is not None else "—",
                "abaixo_min": below,
                "acima_max": above,
                "total_violacoes": total_bad,
                "pct_violacoes": pct,
                "status": "✅ OK" if total_bad == 0 else "❌ FAIL",
            })

        bounds_df = pd.DataFrame(bound_results)
        st.dataframe(
            bounds_df.style.map(
                lambda v: "background-color: #f8b4b4" if v == "❌ FAIL" else "",
                subset=["status"],
            ),
            use_container_width=True,
        )

        for col, min_val, max_val in valid_rules:
            bad_mask = pd.Series(False, index=df.index)
            if min_val is not None:
                bad_mask |= df[col] < min_val
            if max_val is not None:
                bad_mask |= df[col] > max_val
            if bad_mask.sum() > 0:
                st.markdown(f"**Amostra de violações em `{col}`:**")
                st.dataframe(df[bad_mask][[col]].head(20), use_container_width=True)


# ── 5. Tendência de volume ─────────────────────────────────────────────────────
with tab_trend:
    st.subheader("Tendência de Volume (Período a Período)")

    with st.expander("Por que isso importa"):
        st.markdown("""
        Compara a contagem de linhas por período (mês ou ano) com o período anterior.
        Queda brusca indica:
        - Pipeline falhou ao carregar dados para aquele período
        - Janela de extração da origem perdeu registros
        - Bug no filtro de partição cortando dados

        Pico brusco indica:
        - Carga duplicada (pipeline executado duas vezes)
        - Data errada usada na partição

        **Limite de alerta:** sinaliza períodos onde a contagem varia mais que X% em relação ao anterior.
        """)

    col1, col2, col3 = st.columns(3)
    with col1:
        trend_date_col = st.selectbox(
            "Coluna de data",
            [""] + date_like_cols + cols,
            index=1 if date_like_cols else 0,
            key="trend_date",
        )
    with col2:
        trend_granularity = st.selectbox("Granularidade", ["Month", "Year"], key="trend_gran")
    with col3:
        alert_pct = st.number_input(
            "Limite de alerta (%)", min_value=1.0, value=20.0, step=1.0,
            help="Sinaliza período se a contagem variar mais que este % em relação ao anterior.",
        )

    if trend_date_col and st.button("Verificar tendência", key="btn_trend"):
        with st.spinner("Calculando contagens por período…"):
            s = pd.to_datetime(df[trend_date_col], errors="coerce")
            null_dates = int(s.isna().sum())
            if null_dates > 0:
                st.warning(f"{null_dates:,} linhas com datas inválidas excluídas.")

            if trend_granularity == "Month":
                period_series = s.dt.to_period("M").astype(str)
            else:
                period_series = s.dt.year.astype(str)

            trend_df = (
                period_series.value_counts()
                .sort_index()
                .reset_index()
            )
            trend_df.columns = ["periodo", "qtd_linhas"]
            trend_df["qtd_anterior"] = trend_df["qtd_linhas"].shift(1)
            trend_df["variacao_pct"] = (
                (trend_df["qtd_linhas"] - trend_df["qtd_anterior"]) / trend_df["qtd_anterior"] * 100
            ).round(2)
            trend_df["status"] = trend_df["variacao_pct"].apply(
                lambda v: "—" if pd.isna(v) else ("⚠️ ALERTA" if abs(v) > alert_pct else "✅ OK")
            )

        alerts = trend_df[trend_df["status"] == "⚠️ ALERTA"]
        if not alerts.empty:
            st.warning(f"**{len(alerts)} período(s)** com variação > {alert_pct:.0f}%:")
            st.dataframe(alerts, use_container_width=True)
        else:
            st.success(f"Nenhum período com variação > {alert_pct:.0f}%.")

        st.line_chart(trend_df.set_index("periodo")["qtd_linhas"])
        st.dataframe(trend_df, use_container_width=True)
    elif not trend_date_col:
        st.info("Selecione uma coluna de data acima.")
