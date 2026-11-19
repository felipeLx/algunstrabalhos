"""
Validador de Relatórios — Início
Navegue pelo menu lateral para executar as verificações.
"""

import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

st.set_page_config(page_title="Validador de Relatórios", layout="wide")

st.title("Validador de Relatórios")
st.markdown("""
Use o menu lateral para navegar entre as páginas:

| Página | Descrição |
|--------|-----------|
| **AWS Analysis** | Verificação de nulos, timeline semanal e detecção de outliers na tabela S3/Iceberg |
| **Source vs AWS** | Reconciliação de contagem de linhas e schema entre banco de origem e AWS |
| **Data Integrity** | Verificações de integridade: duplicatas, domínio de valores, ordenação de datas, limites numéricos e tendência de volume |

### Como as verificações funcionam
- **Verificação de nulos** — sinaliza colunas que nunca deveriam ser nulas mas estão
- **Timeline semanal** — detecta semanas com contagem de linhas anormalmente baixa (< média − 2σ)
- **Detecção de outliers** — método IQR fence; colunas inteiras exibem Q1/Q3 como inteiro, sem falsa precisão decimal
- **Reconciliação de linhas** — compara `COUNT(*)` do banco de origem vs contagem de linhas no parquet S3, com filtro opcional por mês/ano
- **Diff de schema** — compara nomes de colunas entre banco de origem e AWS

### AWS SSO
Use os botões **Validar Sessão / Login SSO / Limpar Cache** na barra lateral da página AWS Analysis
para gerenciar sua sessão SSO sem sair do app.
""")
