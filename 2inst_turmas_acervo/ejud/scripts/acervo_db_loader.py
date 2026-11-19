import os
import pandas as pd
from dotenv import load_dotenv
from modulo.conexoes import get_postgres_connection, get_oracle_connection_tj05

def print_banner(text): print(f"\n🚩 ====> {text}\n")
def print_info(text): print(f"ℹ️  {text}")
def print_success(text): print(f"✅ {text}")
def print_warning(text): print(f"⚠️  {text}")
def print_error(text): print(f"❌ {text}")

def normaliza_colunas(df):
    df.columns = df.columns.str.strip().str.upper()
    return df

def converte_numericos(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df

def converte_datas(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def limpa_nat_none(row):
    # converte NaT/NaN para None, inclusive para cada linha/valor
    return [None if (pd.isna(v) or str(v) in ['NaT', 'nan']) else v for v in row]

def carga_batch(df, insert_sql, conn, batch_size=10000, is_oracle=False):
    cursor = conn.cursor()
    total_rows = len(df)
    for start_row in range(0, total_rows, batch_size):
        end_row = min(start_row + batch_size, total_rows)
        batch = df.iloc[start_row:end_row]
        # LINHA CHAVE: normaliza cada registro antes do insert
        batch_data = [tuple(limpa_nat_none(row)) for _, row in batch.iterrows()]
        cursor.executemany(insert_sql, batch_data)
        conn.commit()
        print_success(f"Registros {start_row + 1} a {end_row} inseridos com sucesso.")
    cursor.close()
    conn.close()
    print_success("Carga no banco concluída com sucesso!")

def carrega_acervo_hist_pg(parquet_file_path, batch_size=50000):
    print_banner("Iniciando carga no banco acervo_hist (Postgres)")
    df = pd.read_parquet(parquet_file_path)
    df = normaliza_colunas(df)

    numeric_columns = [
        'CODDOC', 'COD_TIPO_PROCESSO', 'COD_COMPETENCIA', 'NUMSEQUENCIAL', 'DV',
        'NUMANO', 'CODJUSTICA', 'CODREGI', 'CODLOCREGI', 'ANO', 'MES', 'DIA', 'COD_ULTIMO_RELATOR','COD_ULTIMO_ORGAO_JULGADOR', 'NIVEL_SIGILO', 'COD_ULT_LOCAL', 'COD_ULTIMO_MOVIMENTO_D01', 'COD_ULTIMO_MOVIMENTO_D01', 'COD_COMP1_D01', 'COD_COMP2_D01', 'COD_COMP3_D01'
    ]
    date_columns = [
        'DTH_REFERENCIA', 'DTH_ULTIMA_DISTRIBUICAO', 'DTH_ULTIMO_MOVIMENTO', 'DTH_ULTIMO_JULGAMENTO',
        'DTH_ENTRADA_LOCAL_VIRTUAL', 'DTH_SUSPENSAO', 'DTH_SOBRESTAMENTO', 'DTHRTERMINOSOBRE',
        'DTH_DISTRIBUICAO', 'DTH_ENTRADA_LOCAL_FISICO', 'DTH_REPERCUSSAO', 'DTH_ULTIMA_DECISAO', 'DTH_ULTIMO_MOVIMENTO_D01'
    ]
    df = converte_numericos(df, numeric_columns)
    df = converte_datas(df, date_columns)

    conn = get_postgres_connection()
    cursor = conn.cursor()

    ano = int(df["ANO"].iloc[0])
    mes = int(df["MES"].iloc[0])
    dia = int(df["DIA"].iloc[0])
    print_info(f"Deletando registros de {ano}-{mes:02d}-{dia:02d} em acervo_hist...")
    cursor.execute(
        "DELETE FROM dw2inst.acervo_hist WHERE ano=%s AND mes=%s AND dia=%s",
        (ano, mes, dia)
    )
    conn.commit()
    print_success(f"Registros do dia {ano}-{mes:02d}-{dia:02d} removidos antes do insert.")

    insert_sql = """
    INSERT INTO dw2inst.acervo_hist (
        dth_referencia, coddoc, dth_ultima_distribuicao, ultimo_relator, ultimo_orgao_julgador,
        ind_eletronico, cod_tipo_processo, cod_competencia, urgente, pedido_liminar, defensoria_publica,
        justica_gratuita, crianca_adolescente, funciona_mp, segredo_justica, idosos, idosos_80_mais,
        doenca_grave, prioridade_deficiente, reu_preso, situacao_rua, classe, numsequencial, dv, numano,
        codjustica, codregi, codlocregi, dth_ultimo_movimento, cod_ultimo_movimento, ultimo_movimento,
        comp1_ultimo_movimento, comp2_ultimo_movimento, comp3_ultimo_movimento, usuario_inclusao,
        dth_ultimo_julgamento, ultimo_julgamento, comp1_ultimo_julgamento, comp2_ultimo_julgamento,
        comp3_ultimo_julgamento, ind_julgado, dth_entrada_local_virtual, local_virtual, cod_assunto,
        assunto, cod_vara, vara, dth_suspensao, movimento_suspensao, comp1_ultima_suspensao,
        comp2_ultima_suspensao, dth_sobrestamento, dthrterminosobre, movimento_sobrestamento,
        comp1_ultimo_sobrestamento, comp2_ultimo_sobrestamento, ind_sobrestado, dth_distribuicao,
        relator, orgao_julgador, dth_entrada_local_fisico, local_fisico, num_processo,
        ano, mes, dia, cod_ultimo_relator, cod_ultimo_orgao_julgador, ind_suspenso, partes, dth_repercussao, comp3_ultima_suspensao, comp3_ultimo_sobrestamento,
        dth_ultima_decisao, ultima_decisao, comp1_ultima_decisao, comp2_ultima_decisao, comp3_ultima_decisao, ind_decisao, nivel_sigilo, cod_ult_local, dth_ultimo_movimento_d01, cod_ultimo_movimento_d01, ultimo_movimento_d01, cod_comp1_d01,comp1_ultimo_movimento_d01, cod_comp2_d01, comp2_ultimo_movimento_d01, cod_comp3_d01, comp3_ultimo_movimento_d01
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (coddoc, ano, mes, dia) DO NOTHING
    """

    campos = [
        'DTH_REFERENCIA', 'CODDOC', 'DTH_ULTIMA_DISTRIBUICAO', 'ULTIMO_RELATOR', 'ULTIMO_ORGAO_JULGADOR',
        'IND_ELETRONICO', 'COD_TIPO_PROCESSO', 'COD_COMPETENCIA', 'URGENTE', 'PEDIDO_LIMINAR', 'DEFENSORIA_PUBLICA',
        'JUSTICA_GRATUITA', 'CRIANCA_ADOLESCENTE', 'FUNCIONA_MP', 'SEGREDO_JUSTICA', 'IDOSOS', 'IDOSOS_80_MAIS',
        'DOENCA_GRAVE', 'PRIORIDADE_DEFICIENTE', 'REU_PRESO', 'SITUACAO_RUA', 'CLASSE', 'NUMSEQUENCIAL', 'DV', 'NUMANO',
        'CODJUSTICA', 'CODREGI', 'CODLOCREGI', 'DTH_ULTIMO_MOVIMENTO', 'COD_ULTIMO_MOVIMENTO', 'ULTIMO_MOVIMENTO',
        'COMP1_ULTIMO_MOVIMENTO', 'COMP2_ULTIMO_MOVIMENTO', 'COMP3_ULTIMO_MOVIMENTO', 'USUARIO_INCLUSAO',
        'DTH_ULTIMO_JULGAMENTO', 'ULTIMO_JULGAMENTO', 'COMP1_ULTIMO_JULGAMENTO', 'COMP2_ULTIMO_JULGAMENTO',
        'COMP3_ULTIMO_JULGAMENTO', 'IND_JULGADO', 'DTH_ENTRADA_LOCAL_VIRTUAL', 'LOCAL_VIRTUAL', 'COD_ASSUNTO',
        'ASSUNTO', 'COD_VARA', 'VARA', 'DTH_SUSPENSAO', 'MOVIMENTO_SUSPENSAO', 'COMP1_ULTIMA_SUSPENSAO',
        'COMP2_ULTIMA_SUSPENSAO', 'DTH_SOBRESTAMENTO', 'DTHRTERMINOSOBRE', 'MOVIMENTO_SOBRESTAMENTO',
        'COMP1_ULTIMO_SOBRESTAMENTO', 'COMP2_ULTIMO_SOBRESTAMENTO', 'IND_SOBRESTADO', 'DTH_DISTRIBUICAO',
        'RELATOR', 'ORGAO_JULGADOR', 'DTH_ENTRADA_LOCAL_FISICO', 'LOCAL_FISICO', 'NUM_PROCESSO',
        'ANO', 'MES', 'DIA', 'COD_ULTIMO_RELATOR', 'COD_ULTIMO_ORGAO_JULGADOR', 'IND_SUSPENSO', 'PARTES', 'DTH_REPERCUSSAO', 'COMP3_ULTIMA_SUSPENSAO', 'COMP3_ULTIMO_SOBRESTAMENTO',
        'DTH_ULTIMA_DECISAO', 'ULTIMA_DECISAO', 'COMP1_ULTIMA_DECISAO', 'COMP2_ULTIMA_DECISAO', 'COMP3_ULTIMA_DECISAO', 'IND_DECISAO', 'NIVEL_SIGILO', 'COD_ULT_LOCAL', 'DTH_ULTIMO_MOVIMENTO_D01', 'COD_ULTIMO_MOVIMENTO_D01', 'ULTIMO_MOVIMENTO_D01', 'COD_COMP1_D01', 'COMP1_ULTIMO_MOVIMENTO_D01', 'COD_COMP2_D01', 'COMP2_ULTIMO_MOVIMENTO_D01', 'COD_COMP3_D01', 'COMP3_ULTIMO_MOVIMENTO_D01'
    ]
    df = df[campos]  # Garante a ordem

    carga_batch(df, insert_sql, conn, batch_size)

def carrega_acervo_relatorio_pg(parquet_file_path, batch_size=50000):
    print_banner("Iniciando carga no banco acervo_relatorio_final (Postgres)")
    df = pd.read_parquet(parquet_file_path)
    df = normaliza_colunas(df)

    numeric_columns = [
        'CODDOC', 'COD_TIPO_PROCESSO', 'COD_COMPETENCIA', 'NUMSEQUENCIAL', 'DV',
        'NUMANO', 'CODJUSTICA', 'CODREGI', 'CODLOCREGI', 'COD_ULTIMO_RELATOR', 'COD_ULTIMO_ORGAO_JULGADOR', 'NIVEL_SIGILO', 'COD_ULT_LOCAL', 'COD_ULTIMO_MOVIMENTO_D01', 'COD_COMP1_D01', 'COD_COMP2_D01', 'COD_COMP3_D01'
    
    ]
    date_columns = [
        'DTH_REFERENCIA', 'DTH_ULTIMA_DISTRIBUICAO', 'DTH_ULTIMO_MOVIMENTO', 'DTH_ULTIMO_JULGAMENTO',
        'DTH_ENTRADA_LOCAL_VIRTUAL', 'DTH_SUSPENSAO', 'DTH_SOBRESTAMENTO', 'DTHRTERMINOSOBRE',
        'DTH_DISTRIBUICAO', 'DTH_ENTRADA_LOCAL_FISICO', 'DTH_REPERCUSSAO', 'DTH_ULTIMA_DECISAO', 'DTH_ULTIMO_MOVIMENTO_D01'
    ]
    df = converte_numericos(df, numeric_columns)
    df = converte_datas(df, date_columns)

    conn = get_postgres_connection()
    cursor = conn.cursor()
    print_info("Truncando a tabela acervo_relatorio_final...")
    cursor.execute("TRUNCATE TABLE dw2inst.acervo_relatorio_final")
    conn.commit()
    print_success("Tabela acervo_relatorio_final truncada com sucesso.")

    insert_sql = """
    INSERT INTO dw2inst.acervo_relatorio_final (
        dth_referencia, coddoc, dth_ultima_distribuicao, ultimo_relator, ultimo_orgao_julgador,
        ind_eletronico, cod_tipo_processo, cod_competencia, urgente, pedido_liminar, defensoria_publica,
        justica_gratuita, crianca_adolescente, funciona_mp, segredo_justica, idosos, idosos_80_mais,
        doenca_grave, prioridade_deficiente, reu_preso, situacao_rua, classe, numsequencial, dv, numano,
        codjustica, codregi, codlocregi, dth_ultimo_movimento, cod_ultimo_movimento, ultimo_movimento,
        comp1_ultimo_movimento, comp2_ultimo_movimento, comp3_ultimo_movimento, usuario_inclusao,
        dth_ultimo_julgamento, ultimo_julgamento, comp1_ultimo_julgamento, comp2_ultimo_julgamento,
        comp3_ultimo_julgamento, ind_julgado, dth_entrada_local_virtual, local_virtual, cod_assunto,
        assunto, cod_vara, vara, dth_suspensao, movimento_suspensao, comp1_ultima_suspensao,
        comp2_ultima_suspensao, dth_sobrestamento, dthrterminosobre, movimento_sobrestamento,
        comp1_ultimo_sobrestamento, comp2_ultimo_sobrestamento, ind_sobrestado, dth_distribuicao,
        relator, orgao_julgador, dth_entrada_local_fisico, local_fisico, num_processo, cod_ultimo_relator, cod_ultimo_orgao_julgador, ind_suspenso, partes, dth_repercussao, comp3_ultima_suspensao, comp3_ultimo_sobrestamento,
        dth_ultima_decisao, ultima_decisao, comp1_ultima_decisao, comp2_ultima_decisao, comp3_ultima_decisao, ind_decisao, nivel_sigilo, cod_ult_local, dth_ultimo_movimento_d01, cod_ultimo_movimento_d01, ultimo_movimento_d01, cod_comp1_d01,comp1_ultimo_movimento_d01, cod_comp2_d01, comp2_ultimo_movimento_d01, cod_comp3_d01, comp3_ultimo_movimento_d01
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (coddoc) DO NOTHING
    """

    campos = [
        'DTH_REFERENCIA', 'CODDOC', 'DTH_ULTIMA_DISTRIBUICAO', 'ULTIMO_RELATOR', 'ULTIMO_ORGAO_JULGADOR',
        'IND_ELETRONICO', 'COD_TIPO_PROCESSO', 'COD_COMPETENCIA', 'URGENTE', 'PEDIDO_LIMINAR', 'DEFENSORIA_PUBLICA',
        'JUSTICA_GRATUITA', 'CRIANCA_ADOLESCENTE', 'FUNCIONA_MP', 'SEGREDO_JUSTICA', 'IDOSOS', 'IDOSOS_80_MAIS',
        'DOENCA_GRAVE', 'PRIORIDADE_DEFICIENTE', 'REU_PRESO', 'SITUACAO_RUA', 'CLASSE', 'NUMSEQUENCIAL', 'DV', 'NUMANO',
        'CODJUSTICA', 'CODREGI', 'CODLOCREGI', 'DTH_ULTIMO_MOVIMENTO', 'COD_ULTIMO_MOVIMENTO', 'ULTIMO_MOVIMENTO',
        'COMP1_ULTIMO_MOVIMENTO', 'COMP2_ULTIMO_MOVIMENTO', 'COMP3_ULTIMO_MOVIMENTO', 'USUARIO_INCLUSAO',
        'DTH_ULTIMO_JULGAMENTO', 'ULTIMO_JULGAMENTO', 'COMP1_ULTIMO_JULGAMENTO', 'COMP2_ULTIMO_JULGAMENTO',
        'COMP3_ULTIMO_JULGAMENTO', 'IND_JULGADO', 'DTH_ENTRADA_LOCAL_VIRTUAL', 'LOCAL_VIRTUAL', 'COD_ASSUNTO',
        'ASSUNTO', 'COD_VARA', 'VARA', 'DTH_SUSPENSAO', 'MOVIMENTO_SUSPENSAO', 'COMP1_ULTIMA_SUSPENSAO',
        'COMP2_ULTIMA_SUSPENSAO', 'DTH_SOBRESTAMENTO', 'DTHRTERMINOSOBRE', 'MOVIMENTO_SOBRESTAMENTO',
        'COMP1_ULTIMO_SOBRESTAMENTO', 'COMP2_ULTIMO_SOBRESTAMENTO', 'IND_SOBRESTADO', 'DTH_DISTRIBUICAO',
        'RELATOR', 'ORGAO_JULGADOR', 'DTH_ENTRADA_LOCAL_FISICO', 'LOCAL_FISICO', 'NUM_PROCESSO', 'COD_ULTIMO_RELATOR', 
        'COD_ULTIMO_ORGAO_JULGADOR', 'IND_SUSPENSO', 'PARTES', 'DTH_REPERCUSSAO', 'COMP3_ULTIMA_SUSPENSAO', 'COMP3_ULTIMO_SOBRESTAMENTO',
        'DTH_ULTIMA_DECISAO', 'ULTIMA_DECISAO', 'COMP1_ULTIMA_DECISAO', 'COMP2_ULTIMA_DECISAO', 'COMP3_ULTIMA_DECISAO', 'IND_DECISAO', 'NIVEL_SIGILO', 'COD_ULT_LOCAL', 'DTH_ULTIMO_MOVIMENTO_D01','COD_ULTIMO_MOVIMENTO_D01', 'ULTIMO_MOVIMENTO_D01', 'COD_COMP1_D01', 'COMP1_ULTIMO_MOVIMENTO_D01', 'COD_COMP2_D01', 'COMP2_ULTIMO_MOVIMENTO_D01', 'COD_COMP3_D01', 'COMP3_ULTIMO_MOVIMENTO_D01'
    ]
    df = df[campos]  # Garante a ordem

    carga_batch(df, insert_sql, conn, batch_size)

def carrega_turmas_relatorio_pg(parquet_file_path, batch_size=50000):
    print_banner("Iniciando carga no banco turmas_relatorio_final (Postgres)")
    df = pd.read_parquet(parquet_file_path)
    df = normaliza_colunas(df)

    numeric_columns = [
        'CODDOC', 'COD_TIPO_PROCESSO', 'COD_COMPETENCIA', 'NUMSEQUENCIAL', 'DV',
        'NUMANO', 'CODJUSTICA', 'CODREGI', 'CODLOCREGI', 'COD_ULTIMO_RELATOR', 'COD_ULTIMO_ORGAO_JULGADOR', 'NIVEL_SIGILO', 'COD_ULT_LOCAL', 'COD_ULTIMO_MOVIMENTO_D01', 'COD_COMP1_D01', 'COD_COMP2_D01', 'COD_COMP3_D01'
    
    ]
    date_columns = [
        'DTH_REFERENCIA', 'DTH_ULTIMA_DISTRIBUICAO', 'DTH_ULTIMO_MOVIMENTO', 'DTH_ULTIMO_JULGAMENTO',
        'DTH_ENTRADA_LOCAL_VIRTUAL', 'DTH_SUSPENSAO', 'DTH_SOBRESTAMENTO', 'DTHRTERMINOSOBRE',
        'DTH_DISTRIBUICAO', 'DTH_ENTRADA_LOCAL_FISICO', 'DTH_REPERCUSSAO', 'DTH_ULTIMA_DECISAO', 'DTH_ULTIMO_MOVIMENTO_D01'
    ]
    df = converte_numericos(df, numeric_columns)
    df = converte_datas(df, date_columns)

    print_info(f"Total de registros turmas lidos do parquet: {len(df)}")

    dth_ref = df['DTH_REFERENCIA'].iloc[0] if len(df) > 0 else None

    conn = get_postgres_connection()
    cursor = conn.cursor()
    if dth_ref is not None:
        print_info(f"Deletando registros de dth_referencia={dth_ref}...")
        cursor.execute(
            "DELETE FROM dw2inst.turmas_relatorio_final WHERE dth_referencia = %s",
            (str(dth_ref),)
        )
        conn.commit()
        print_success(f"Registros de dth_referencia={dth_ref} removidos.")

    insert_sql = """
    INSERT INTO dw2inst.turmas_relatorio_final (
        dth_referencia, coddoc, dth_ultima_distribuicao, ultimo_relator, ultimo_orgao_julgador,
        ind_eletronico, cod_tipo_processo, cod_competencia, urgente, pedido_liminar, defensoria_publica,
        justica_gratuita, crianca_adolescente, funciona_mp, segredo_justica, idosos, idosos_80_mais,
        doenca_grave, prioridade_deficiente, reu_preso, situacao_rua, classe, numsequencial, dv, numano,
        codjustica, codregi, codlocregi, dth_ultimo_movimento, cod_ultimo_movimento, ultimo_movimento,
        comp1_ultimo_movimento, comp2_ultimo_movimento, comp3_ultimo_movimento, usuario_inclusao,
        dth_ultimo_julgamento, ultimo_julgamento, comp1_ultimo_julgamento, comp2_ultimo_julgamento,
        comp3_ultimo_julgamento, ind_julgado, dth_entrada_local_virtual, local_virtual, cod_assunto,
        assunto, cod_vara, vara, dth_suspensao, movimento_suspensao, comp1_ultima_suspensao,
        comp2_ultima_suspensao, dth_sobrestamento, dthrterminosobre, movimento_sobrestamento,
        comp1_ultimo_sobrestamento, comp2_ultimo_sobrestamento, ind_sobrestado, dth_distribuicao,
        relator, orgao_julgador, dth_entrada_local_fisico, local_fisico, num_processo, cod_ultimo_relator, cod_ultimo_orgao_julgador, ind_suspenso, partes, dth_repercussao, comp3_ultima_suspensao, comp3_ultimo_sobrestamento,
        dth_ultima_decisao, ultima_decisao, comp1_ultima_decisao, comp2_ultima_decisao, comp3_ultima_decisao, ind_decisao, nivel_sigilo, cod_ult_local, dth_ultimo_movimento_d01, cod_ultimo_movimento_d01, ultimo_movimento_d01, cod_comp1_d01,comp1_ultimo_movimento_d01, cod_comp2_d01, comp2_ultimo_movimento_d01, cod_comp3_d01, comp3_ultimo_movimento_d01
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (coddoc, dth_referencia) DO NOTHING
    """

    campos = [
        'DTH_REFERENCIA', 'CODDOC', 'DTH_ULTIMA_DISTRIBUICAO', 'ULTIMO_RELATOR', 'ULTIMO_ORGAO_JULGADOR',
        'IND_ELETRONICO', 'COD_TIPO_PROCESSO', 'COD_COMPETENCIA', 'URGENTE', 'PEDIDO_LIMINAR', 'DEFENSORIA_PUBLICA',
        'JUSTICA_GRATUITA', 'CRIANCA_ADOLESCENTE', 'FUNCIONA_MP', 'SEGREDO_JUSTICA', 'IDOSOS', 'IDOSOS_80_MAIS',
        'DOENCA_GRAVE', 'PRIORIDADE_DEFICIENTE', 'REU_PRESO', 'SITUACAO_RUA', 'CLASSE', 'NUMSEQUENCIAL', 'DV', 'NUMANO',
        'CODJUSTICA', 'CODREGI', 'CODLOCREGI', 'DTH_ULTIMO_MOVIMENTO', 'COD_ULTIMO_MOVIMENTO', 'ULTIMO_MOVIMENTO',
        'COMP1_ULTIMO_MOVIMENTO', 'COMP2_ULTIMO_MOVIMENTO', 'COMP3_ULTIMO_MOVIMENTO', 'USUARIO_INCLUSAO',
        'DTH_ULTIMO_JULGAMENTO', 'ULTIMO_JULGAMENTO', 'COMP1_ULTIMO_JULGAMENTO', 'COMP2_ULTIMO_JULGAMENTO',
        'COMP3_ULTIMO_JULGAMENTO', 'IND_JULGADO', 'DTH_ENTRADA_LOCAL_VIRTUAL', 'LOCAL_VIRTUAL', 'COD_ASSUNTO',
        'ASSUNTO', 'COD_VARA', 'VARA', 'DTH_SUSPENSAO', 'MOVIMENTO_SUSPENSAO', 'COMP1_ULTIMA_SUSPENSAO',
        'COMP2_ULTIMA_SUSPENSAO', 'DTH_SOBRESTAMENTO', 'DTHRTERMINOSOBRE', 'MOVIMENTO_SOBRESTAMENTO',
        'COMP1_ULTIMO_SOBRESTAMENTO', 'COMP2_ULTIMO_SOBRESTAMENTO', 'IND_SOBRESTADO', 'DTH_DISTRIBUICAO',
        'RELATOR', 'ORGAO_JULGADOR', 'DTH_ENTRADA_LOCAL_FISICO', 'LOCAL_FISICO', 'NUM_PROCESSO', 'COD_ULTIMO_RELATOR', 
        'COD_ULTIMO_ORGAO_JULGADOR', 'IND_SUSPENSO', 'PARTES', 'DTH_REPERCUSSAO', 'COMP3_ULTIMA_SUSPENSAO', 'COMP3_ULTIMO_SOBRESTAMENTO',
        'DTH_ULTIMA_DECISAO', 'ULTIMA_DECISAO', 'COMP1_ULTIMA_DECISAO', 'COMP2_ULTIMA_DECISAO', 'COMP3_ULTIMA_DECISAO', 'IND_DECISAO', 'NIVEL_SIGILO', 'COD_ULT_LOCAL', 'DTH_ULTIMO_MOVIMENTO_D01','COD_ULTIMO_MOVIMENTO_D01', 'ULTIMO_MOVIMENTO_D01', 'COD_COMP1_D01', 'COMP1_ULTIMO_MOVIMENTO_D01', 'COD_COMP2_D01', 'COMP2_ULTIMO_MOVIMENTO_D01', 'COD_COMP3_D01', 'COMP3_ULTIMO_MOVIMENTO_D01'
    ]
    df = df[campos]  # Garante a ordem

    carga_batch(df, insert_sql, conn, batch_size)


# Mantive a Oracle igual (se precisar rodar, só copiar o mesmo padrão do Postgres)

if __name__ == "__main__":
    PARQUET_HOME = os.environ.get("PARQUET_HOME", "/opt/parquet")
    parquet_relatorio = os.path.join(PARQUET_HOME, "data", "ouro", "dw2inst", "acervo_relatorio_final")
    parquet_turmas_relatorio = os.path.join(PARQUET_HOME, "data", "ouro", "dw2inst", "turmas_relatorio_final")

    carrega_acervo_relatorio_pg(parquet_relatorio, batch_size=10000)
    carrega_turmas_relatorio_pg(parquet_turmas_relatorio, batch_size=10000)
    
    # carrega_acervo_relatorio_oracle(parquet_relatorio, batch_size=10000) # Se quiser Oracle, mesma lógica!


