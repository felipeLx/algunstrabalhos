import sys
import json
import boto3
import psycopg2
import logging
import re
from datetime import datetime, timedelta
import pytz
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType

# --- CONFIGURAÇÕES ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ID_PROJETO = 1
CATALOG = "glue_catalog"
SCHEMA_ALVO = "dw2inst"
FUSO_BR = pytz.timezone('America/Sao_Paulo')
AWS_REGION = "sa-east-1"
HOME_EXECUTOR = f"s3://sgdai-projetos/2instancia/2inst_acervo_aws/scripts/acervo_ejud.py"

S3_BASE_PROJETO = "s3://sgdai-projetos/2instancia/2inst_acervo_aws"
S3_SQL_PRATA = f"{S3_BASE_PROJETO}/consultas/query_acervo_ejud.sql"
S3_SQL_OURO = f"{S3_BASE_PROJETO}/consultas/query_acervo_indicadores_ejud.sql"
S3_JSON_TABELAS = f"{S3_BASE_PROJETO}/json/tabelas_acervo.json"

TABELA_PRATA = f"{CATALOG}.{SCHEMA_ALVO}.acervo_ejud"
TABELA_DIARIA = f"{CATALOG}.{SCHEMA_ALVO}.tb_acervo_indicadores_ejud"
TABELA_HIST  = f"{CATALOG}.{SCHEMA_ALVO}.tb_acervo_hist_indicadores_ejud"
TABELA_FATO   = f"{CATALOG}.{SCHEMA_ALVO}.tf_acervo_ejud"

MVIEW_SHUFFLE_PARTITIONS = 200 
MVIEW_OUTPUT_FILES = 4

# --- FUNÇÕES AUXILIARES ---

def get_postgres_connection():
    secret_name = "producao/rds/dw01/postgres"
    client = boto3.client(service_name='secretsmanager', region_name=AWS_REGION)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return psycopg2.connect(
        host=secret['host'], database=secret['dbname'],
        user=secret['username'], password=secret['password'],
        port=int(secret['port'])
    )

def registrar_inicio_carga(id_projeto, schema, tabela, dt_referencia, home_executor):
    agora_br_str = datetime.now(FUSO_BR).strftime('%Y-%m-%d %H:%M:%S.%f')
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dbcontrole.controle_execucao_carga_projeto WHERE id_projeto = %s AND schema_nome = %s AND tabela_nome = %s AND dt_referencia = %s", (id_projeto, schema, tabela, dt_referencia))
            cur.execute("INSERT INTO dbcontrole.controle_execucao_carga_projeto (id_projeto, schema_nome, tabela_nome, dt_carga, dt_ini_carga, status, home_script_executor, dt_referencia) VALUES (%s, %s, %s, %s, %s::timestamp, %s, %s, %s)", (id_projeto, schema, tabela, dt_referencia.split(' ')[0], agora_br_str, "INICIADA", home_executor, dt_referencia))
            conn.commit()
    finally: conn.close()

def registrar_execucao_final(id_projeto, schema, tabela, dt_referencia, qt_registros, dt_ini_py, status, erro=None):
    agora_br = datetime.now(FUSO_BR)
    agora_br_str = agora_br.strftime('%Y-%m-%d %H:%M:%S.%f')
    tempo_exec_formatado = str(agora_br - dt_ini_py).split('.')[0]
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE dbcontrole.controle_execucao_carga_projeto SET status = %s, dt_fim_carga = %s::timestamp, qt_registros = %s, tempo_execucao = %s, mensagem_erro = %s WHERE id_projeto = %s AND schema_nome = %s AND tabela_nome = %s AND dt_referencia = %s", (status, agora_br_str, qt_registros, tempo_exec_formatado, erro, id_projeto, schema, tabela, dt_referencia))
            conn.commit()
    finally: conn.close()

def align_df_to_target_table(df, table_name, spark_session):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    target_schema = spark_session.table(table_name).schema
    for field in target_schema.fields:
        f_name = field.name.lower()
        if f_name not in df.columns:
            df = df.withColumn(f_name, F.lit(None).cast(field.dataType))
    return df.select(*[f.name.lower() for f in target_schema.fields])

def normalize_sql_catalog_names(sql_text: str) -> str:
    sql_text = re.sub(r'\bEJUD\.([A-Z_0-9]+)\b', lambda m: f"ejud.{m.group(1).lower()}", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bDW2INST\.([A-Z_0-9]+)\b', lambda m: f"{SCHEMA_ALVO}.{m.group(1).lower()}", sql_text, flags=re.IGNORECASE)
    return sql_text

def get_s3_content(path: str) -> str:
    s3 = boto3.client('s3', region_name=AWS_REGION)
    bucket = path.split('/')[2]
    key = '/'.join(path.split('/')[3:])
    return s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

# --- MAIN ---
dt_inicio_global_py = datetime.now(FUSO_BR)
mode = "diario"
ref_date_arg = None

if "--modo" in sys.argv: mode = sys.argv[sys.argv.index("--modo") + 1]
if "--ref_date" in sys.argv:
    try:
        date_raw = sys.argv[sys.argv.index("--ref_date") + 1].replace(')', '').strip()
        ref_date_arg = datetime.strptime(date_raw.split(' ')[0], '%Y-%m-%d')
    except: ref_date_arg = None

if mode == "historico":
    if ref_date_arg is None: ref_date_arg = (dt_inicio_global_py - timedelta(days=1)).replace(tzinfo=None)
    v_dateref = datetime(ref_date_arg.year, ref_date_arg.month, ref_date_arg.day, 23, 59, 59)
    v_dateref_str = v_dateref.strftime('%Y-%m-%d %H:%M:%S')
    ano, mes, dia = v_dateref.year, v_dateref.month, v_dateref.day
else:
    v_dateref_str = dt_inicio_global_py.strftime('%Y-%m-%d %H:%M:%S')
    ano = mes = dia = None

TABELA_ALVO_LOG = TABELA_HIST if mode == "historico" else TABELA_DIARIA
registrar_inicio_carga(ID_PROJETO, SCHEMA_ALVO, TABELA_ALVO_LOG.replace(f"{CATALOG}.", ""), v_dateref_str, HOME_EXECUTOR)

spark = (SparkSession.builder
    .appName(f"Acervo_Ejud_v6_{mode}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.shuffle.partitions", str(MVIEW_SHUFFLE_PARTITIONS))
    .getOrCreate())

total_gravado = 0
status_processo = "SUCESSO"
msg_erro = None

try:
    # 1. PRATA
    sql_p = normalize_sql_catalog_names(get_s3_content(S3_SQL_PRATA).replace("{v_dateref_str}", v_dateref_str))
    df_p = spark.sql(sql_p).withColumn("datareferencia", F.lit(v_dateref_str).cast("timestamp_ntz"))
    df_p = align_df_to_target_table(df_p, TABELA_PRATA, spark)
    df_p.writeTo(TABELA_PRATA).using("iceberg").createOrReplace()
    df_p.createOrReplaceTempView("acervo_ejud")

    # 2. OURO
    js = json.loads(get_s3_content(S3_JSON_TABELAS))
    for t in js['tabelas']: spark.read.table(f"{CATALOG}.{js['schema_nome']}.{t}").createOrReplaceTempView(t)

    sql_o = normalize_sql_catalog_names(get_s3_content(S3_SQL_OURO).replace("{v_dateref_str}", v_dateref_str))
    df_o = spark.sql(sql_o).persist()

    if mode == "historico":
        df_f = df_o.withColumn("dth_referencia", F.lit(v_dateref_str)).withColumn("ano", F.lit(ano).cast("int")).withColumn("mes", F.lit(mes).cast("int")).withColumn("dia", F.lit(dia).cast("int"))
        df_f = align_df_to_target_table(df_f, TABELA_HIST, spark)
        total_gravado = df_f.count()
        spark.sql(f"DELETE FROM {TABELA_HIST} WHERE ano={int(ano)} AND mes={int(mes)} AND dia={int(dia)}")
        df_f.writeTo(TABELA_HIST).append()
    else:
        df_f = align_df_to_target_table(df_o.withColumn("dth_referencia", F.lit(v_dateref_str)), TABELA_DIARIA, spark)
        total_gravado = df_f.count()
        df_f.writeTo(TABELA_DIARIA).using("iceberg").createOrReplace()

    # --- FASE 3: ATUALIZAÇÃO DA TABELA FATO (REFRESH MVIEW) ---
    logger.info("Iniciando Refresh da Tabela Fato (MVIEW)...")
    hoje_dt = dt_inicio_global_py.date()
    schema_fato = spark.table(TABELA_FATO).schema
    cols_fato = [f.name for f in schema_fato.fields]
    cols_meta = {"ano", "mes", "dia", "tipo", "dt_base", "fonte"}
    cols_negocio = [c for c in cols_fato if c not in cols_meta]

    # Foto Histórica (Último dia de cada mês + dia do ano anterior)
    df_hist_src = spark.read.table(TABELA_HIST)
    df_hist_mview = df_hist_src.withColumn("dth_ref_ts", F.to_timestamp(F.col("dth_referencia"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("tipo", F.when(F.dayofmonth(F.col("dth_ref_ts")) == F.dayofmonth(F.last_day(F.col("dth_ref_ts"))), F.lit("ULTIMO_DIA_MES"))
                    .when((F.year(F.col("dth_ref_ts")) == hoje_dt.year - 1) & (F.month(F.col("dth_ref_ts")) == hoje_dt.month) & (F.dayofmonth(F.col("dth_ref_ts")) == hoje_dt.day), F.lit("DIA_ANO_ANTERIOR"))) \
        .filter(F.col("tipo").isNotNull()) \
        .withColumn("dt_base", F.to_date(F.col("dth_ref_ts"))) \
        .withColumn("fonte", F.lit("tb_acervo_hist_indicadores_ejud"))

    # Foto Atual (DIA_ATUAL)
    df_foto_atual = df_o.withColumn("dth_ref_ts", F.to_timestamp(F.lit(v_dateref_str), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("ano", F.year(F.col("dth_ref_ts")).cast("int")) \
        .withColumn("mes", F.month(F.col("dth_ref_ts")).cast("int")) \
        .withColumn("dia", F.dayofmonth(F.col("dth_ref_ts")).cast("int")) \
        .withColumn("tipo", F.lit("DIA_ATUAL")) \
        .withColumn("dt_base", F.lit(hoje_dt).cast("date")) \
        .withColumn("fonte", F.lit("tb_acervo_indicadores_ejud"))

    # Unifica e Alinha com a Fato
    df_mview_final = df_hist_mview.select(*[c for c in df_hist_mview.columns if c in cols_fato or c == "dth_referencia"]) \
        .unionByName(df_foto_atual.select(*[c for c in df_foto_atual.columns if c in cols_fato or c == "dth_referencia"]), allowMissingColumns=True)
    
    df_mview_final = align_df_to_target_table(df_mview_final, TABELA_FATO, spark)
    df_mview_final.writeTo(TABELA_FATO).overwritePartitions()
    
    logger.info("Refresh da Fato concluído com sucesso!")

except Exception as e:
    status_processo = "ERRO"; msg_erro = str(e); logger.error(f"ERRO: {msg_erro}"); raise
finally:
    registrar_execucao_final(ID_PROJETO, SCHEMA_ALVO, TABELA_ALVO_LOG.replace(f"{CATALOG}.", ""), v_dateref_str, total_gravado, dt_inicio_global_py, status_processo, msg_erro)
    spark.stop()
