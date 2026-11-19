import sys
import json
import boto3
import psycopg2
import logging
from datetime import datetime, timedelta
import pytz
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType

# --- CONFIGURAÇÕES DE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONSTANTES E TIMEZONE ---
ID_PROJETO = 5
SCHEMA_ALVO = "dw2inst"
HOME_EXECUTOR = "s3://sgdai-projetos/2instancia/2inst_acervo_aws/scripts/acervo_eproc.py"
FUSO_BR = pytz.timezone('America/Sao_Paulo')

# --- 1. FUNÇÕES DE CONEXÃO E CONTROLE DE CARGA ---

def get_postgres_connection():
    """Busca credenciais no Secrets Manager e conecta ao RDS Postgres"""
    secret_name = "producao/rds/dw01/postgres"
    region_name = "sa-east-1"
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return psycopg2.connect(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password'],
            port=int(secret['port'])
        )
    except Exception as e:
        logger.error(f"Erro ao conectar no Postgres via Secrets: {e}")
        raise e


def registrar_inicio_carga(id_projeto, schema, tabela, dt_referencia, home_executor):
    agora_br_str = datetime.now(FUSO_BR).strftime('%Y-%m-%d %H:%M:%S.%f')
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM dbcontrole.controle_execucao_carga_projeto
                WHERE id_projeto = %s AND schema_nome = %s AND tabela_nome = %s AND dt_referencia = %s
            """, (id_projeto, schema, tabela, dt_referencia))

            cur.execute("""
                INSERT INTO dbcontrole.controle_execucao_carga_projeto
                (id_projeto, schema_nome, tabela_nome, dt_carga, dt_ini_carga, status, home_script_executor, dt_referencia)
                VALUES (%s, %s, %s, %s, %s::timestamp, %s, %s, %s)
            """, (id_projeto, schema, tabela, dt_referencia.split(' ')[0], agora_br_str, "INICIADA", home_executor, dt_referencia))
            conn.commit()
    finally:
        conn.close()


def registrar_execucao_final(id_projeto, schema, tabela, dt_referencia, qt_registros, dt_ini_py, status, erro=None):
    agora_br = datetime.now(FUSO_BR)
    agora_br_str = agora_br.strftime('%Y-%m-%d %H:%M:%S.%f')
    tempo_exec_raw = agora_br - dt_ini_py
    tempo_exec_formatado = str(tempo_exec_raw).split('.')[0]

    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE dbcontrole.controle_execucao_carga_projeto
                SET status = %s, dt_fim_carga = %s::timestamp, qt_registros = %s, tempo_execucao = %s, mensagem_erro = %s
                WHERE id_projeto = %s AND schema_nome = %s AND tabela_nome = %s AND dt_referencia = %s
            """, (status, agora_br_str, qt_registros, tempo_exec_formatado, erro, id_projeto, schema, tabela, dt_referencia))
            conn.commit()
    finally:
        conn.close()


# --- 2. FUNÇÕES AUXILIARES ---

def get_s3_content(path):
    s3 = boto3.client('s3')
    bucket = path.split('/')[2]
    key = '/'.join(path.split('/')[3:])
    return s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')


def cast_timestamp_columns_to_ntz(df, exclude_cols=None):
    """
    Padroniza colunas timestamp para timestamp_ntz.
    Pode excluir colunas específicas que não devam ser recastadas.
    """
    exclude_cols = set(exclude_cols or [])
    timestamp_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, TimestampType) and field.name not in exclude_cols
    ]

    if timestamp_cols:
        logger.info(f"Normalizando colunas timestamp para timestamp_ntz: {', '.join(timestamp_cols)}")
        for col_name in timestamp_cols:
            df = df.withColumn(col_name, F.col(col_name).cast("timestamp_ntz"))
    else:
        logger.info("Nenhuma coluna timestamp encontrada para normalização.")

    return df


def add_dth_referencia_as_string(df, dth_ref_str):
    """
    Grava dth_referencia como STRING no formato yyyy-MM-dd HH:mm:ss,
    exatamente como deve aparecer no Athena/Qlik.
    """
    return df.withColumn("dth_referencia", F.lit(dth_ref_str).cast("string"))


def resolver_data_referencia(modo, ref_date_raw=None):
    """
    Resolve a data de referência para os modos:
    - historico: usa a data informada e fecha em 23:59:59 quando vier só YYYY-MM-DD
    - diario: usa o timestamp atual da execução
    Retorna:
    - v_dateref_str: string no formato YYYY-MM-DD HH:MM:SS
    - ano, mes, dia: inteiros quando modo historico, senão None
    """
    modo = (modo or "diario").strip().lower()

    if modo == "historico":
        if ref_date_raw:
            ref_date_raw = ref_date_raw.strip().replace(')', '')
        else:
            ref_date_raw = (datetime.now(FUSO_BR) - timedelta(days=1)).strftime('%Y-%m-%d')

        if len(ref_date_raw) == 10:
            v_dateref_str = f"{ref_date_raw} 23:59:59"
        else:
            v_dateref_str = ref_date_raw

        try:
            dt_ref = datetime.strptime(v_dateref_str, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            raise ValueError(
                f"ref_date inválida: '{ref_date_raw}'. Use 'YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS'."
            ) from e

        return v_dateref_str, dt_ref.year, dt_ref.month, dt_ref.day

    if modo == "diario":
        agora = datetime.now(FUSO_BR)
        return agora.strftime('%Y-%m-%d %H:%M:%S'), None, None, None

    raise ValueError("Modo inválido. Use 'historico' ou 'diario'.")


# --- 3. LÓGICA DE INICIALIZAÇÃO E ARGUMENTOS ---

dt_inicio_global_py = datetime.now(FUSO_BR)
mode = "diario"
ref_date_arg = None

if "--modo" in sys.argv:
    mode = sys.argv[sys.argv.index("--modo") + 1].strip().lower()

if "--ref_date" in sys.argv:
    try:
        ref_date_arg = sys.argv[sys.argv.index("--ref_date") + 1]
        logger.info(f"DATA_REF CAPTURADA DO SHELL: {ref_date_arg}")
    except Exception as e:
        logger.error(f"Erro ao capturar ref_date: {e}")
        ref_date_arg = None

v_dateref_str, ano, mes, dia = resolver_data_referencia(mode, ref_date_arg)

logger.info(f"Modo de execução: {mode}")
logger.info(f"Data de referência utilizada: {v_dateref_str}")

TABELA_ALVO = "tb_acervo_hist_indicadores_eproc2g" if mode == "historico" else "tb_acervo_indicadores_eproc2g"

registrar_inicio_carga(ID_PROJETO, SCHEMA_ALVO, TABELA_ALVO, v_dateref_str, HOME_EXECUTOR)

# --- 4. PROCESSAMENTO SPARK ---

spark = SparkSession.builder \
    .appName(f"Acervo_Eproc_{mode}") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .config("spark.sql.timestampType", "TIMESTAMP_NTZ") \
    .getOrCreate()

status_processo = "SUCESSO"
msg_erro = None
total_gravado = 0

try:
    # FASE 1: Prata
    logger.info(f"Fase Prata: Referência {v_dateref_str}")
    sql_prata_raw = get_s3_content("s3://sgdai-projetos/2instancia/2inst_acervo_aws/consultas/query_acervo_eproc2g.sql")
    sql_prata_final = sql_prata_raw.replace("{v_dateref_str}", v_dateref_str)

    df_prata = spark.sql(sql_prata_final) \
        .withColumn("datareferencia", F.lit(v_dateref_str).cast("timestamp_ntz"))
    df_prata = cast_timestamp_columns_to_ntz(df_prata)

    df_prata.writeTo(f"{SCHEMA_ALVO}.tb_acervo_eproc2g").using("iceberg").createOrReplace()
    df_prata.createOrReplaceTempView("tb_acervo_eproc2g")

    # FASE 2: Dependências Ouro
    config = json.loads(get_s3_content("s3://sgdai-projetos/2instancia/2inst_acervo_aws/json/tabelas_acervo.json"))
    for tab in config['tabelas']:
        spark.read.table(f"{config['schema_nome']}.{tab}").createOrReplaceTempView(tab)

    # FASE 3: Query Ouro
    sql_ouro_raw = get_s3_content("s3://sgdai-projetos/2instancia/2inst_acervo_aws/consultas/query_acervo_indicadores_eproc2g.sql")
    sql_ouro_final = sql_ouro_raw.replace("{v_dateref_str}", v_dateref_str).replace("VARCHAR", "STRING")
    df_ouro = spark.sql(sql_ouro_final)
    df_ouro = cast_timestamp_columns_to_ntz(df_ouro, exclude_cols=["dth_referencia"])

    # FASE 4: Gravação
    if mode == "historico":
        df_final = (
            df_ouro
            .transform(lambda df: add_dth_referencia_as_string(df, v_dateref_str))
            .withColumn("ano", F.lit(ano).cast("int"))
            .withColumn("mes", F.lit(mes).cast("int"))
            .withColumn("dia", F.lit(dia).cast("int"))
        )

        total_gravado = df_final.count()
        spark.sql(f"DELETE FROM {SCHEMA_ALVO}.{TABELA_ALVO} WHERE ano = {int(ano)} AND mes = {int(mes)} AND dia = {int(dia)}")
        df_final.writeTo(f"{SCHEMA_ALVO}.{TABELA_ALVO}").option("mergeSchema", "true").append()
    else:
        df_ouro_diario = (
            df_ouro
            .transform(lambda df: add_dth_referencia_as_string(df, v_dateref_str))
            .withColumn("dth_referencia_ts", F.to_timestamp("dth_referencia"))
            .withColumn("ano", F.year("dth_referencia_ts"))
            .withColumn("mes", F.month("dth_referencia_ts"))
            .withColumn("dia", F.dayofmonth("dth_referencia_ts"))
            .drop("dth_referencia_ts")
        )

        total_gravado = df_ouro_diario.count()
        tabela_diaria = f"{SCHEMA_ALVO}.{TABELA_ALVO}"

        if spark.catalog.tableExists(tabela_diaria):
            logger.info(f"Tabela diária {tabela_diaria} já existe. Limpando dados antes do append...")
            spark.sql(f"DELETE FROM {tabela_diaria}")
            df_ouro_diario.writeTo(tabela_diaria).append()
        else:
            logger.info(f"Tabela diária {tabela_diaria} não existe. Criando tabela Iceberg...")
            df_ouro_diario.writeTo(tabela_diaria).using("iceberg").create()

    # FASE 5: REFRESH MVIEW
    hoje_dt = dt_inicio_global_py.date()
    df_hist_mview = spark.read.table(f"{SCHEMA_ALVO}.tb_acervo_hist_indicadores_eproc2g")
    df_hist_mview = cast_timestamp_columns_to_ntz(df_hist_mview, exclude_cols=["dth_referencia"])

    # dth_referencia agora é STRING na tabela histórica; convertemos apenas para uso interno na mview
    df_hist_mview = df_hist_mview.withColumn("dth_referencia_ts", F.to_timestamp(F.col("dth_referencia"), "yyyy-MM-dd HH:mm:ss"))

    df_fotos_hist = (
        df_hist_mview
        .withColumn(
            "tipo",
            F.when(
                F.dayofmonth(F.col("dth_referencia_ts")) == F.dayofmonth(F.last_day(F.col("dth_referencia_ts"))),
                "ULTIMO_DIA_MES"
            )
            .when(
                (F.year(F.col("dth_referencia_ts")) == hoje_dt.year - 1) &
                (F.month(F.col("dth_referencia_ts")) == hoje_dt.month) &
                (F.dayofmonth(F.col("dth_referencia_ts")) == hoje_dt.day),
                "DIA_ANO_ANTERIOR"
            )
            .otherwise(None)
        )
        .filter(F.col("tipo").isNotNull())
        .withColumn("dt_base", F.to_date(F.col("dth_referencia_ts")))
        .withColumn("fonte", F.lit("tb_acervo_hist_indicadores_eproc2g"))
        .drop("dth_referencia_ts")
    )


    df_foto_atual = (
        spark.read.table(f"{SCHEMA_ALVO}.tb_acervo_indicadores_eproc2g")
        .transform(lambda df: cast_timestamp_columns_to_ntz(df, exclude_cols=["dth_referencia"]))
        .withColumn("dth_referencia_ts", F.to_timestamp(F.col("dth_referencia")))
        .withColumn("ano", F.year(F.col("dth_referencia_ts")))
        .withColumn("mes", F.month(F.col("dth_referencia_ts")))
        .withColumn("dia", F.dayofmonth(F.col("dth_referencia_ts")))
        .withColumn("tipo", F.lit("DIA_ATUAL"))
        .withColumn("dt_base", F.lit(hoje_dt))
        .withColumn("fonte", F.lit("tb_acervo_indicadores_eproc2g"))
        .drop("dth_referencia_ts")
    )

    df_mview_final = df_fotos_hist.unionByName(df_foto_atual, allowMissingColumns=True)
    df_mview_final.writeTo(f"{SCHEMA_ALVO}.tf_acervo_eproc").using("iceberg").createOrReplace()

    logger.info("Carga concluída com sucesso!")

except Exception as e:
    status_processo = "ERRO"
    msg_erro = str(e)
    logger.error(f"Erro fatal: {msg_erro}")
    raise e
finally:
    registrar_execucao_final(
        ID_PROJETO,
        SCHEMA_ALVO,
        TABELA_ALVO,
        v_dateref_str,
        total_gravado,
        dt_inicio_global_py,
        status_processo,
        msg_erro
    )


