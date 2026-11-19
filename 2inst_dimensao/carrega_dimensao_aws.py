# carrega_dimensao_aws.py
# -*- coding: utf-8 -*-
"""
Gera dimensões do sistema EJUD em um ambiente AWS S3 com Spark.
- Lê fontes de dados (bronze, prata, ouro) do S3.
- Executa a SQL de cada dimensão.
- Normaliza, renomeia, aplica casts e defaults via Spark.
- Deduz registros duplicados por PK.
- Salva o resultado como um arquivo Parquet único no S3.
- (Opcional) Carrega o resultado final em um banco de dados Postgres via JDBC.
"""
import argparse
import os
import json
from datetime import datetime
import logging

# ==== Módulos customizados (devem ser enviados com --py-files no spark-submit) ====
from modulo.conexoes import get_postgres_connection
from modulo.controle_carga import registrar_inicio_carga, registrar_fim_carga

# ==== Configurações do Ambiente (via variáveis de ambiente) ====
# Flag para controlar a carga final no banco de dados Postgres
LOAD_TO_DB = os.environ.get("LOAD_TO_DB", "true").lower() in ("1", "true", "yes")

# Buckets e prefixos do Data Lake
BUCKET_BRONZE = os.environ.get("BUCKET_BRONZE", "sgdai-data-lake")
BUCKET_PRATA = os.environ.get("BUCKET_PRATA", "sgdai-data-lake")
BUCKET_OURO = os.environ.get("BUCKET_OURO", "sgdai-data-lake")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze")
PRATA_PREFIX = os.environ.get("PRATA_PREFIX", "prata/dim2inst")
OURO_PREFIX = os.environ.get("OURO_PREFIX", "ouro/dim2inst")

# Credenciais do Postgres para o conector JDBC e para o módulo de controle
PG_JDBC_URL = os.environ.get("PG_JDBC_URL") # Ex: "jdbc:postgresql://host:port/database"
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# Caminho para o arquivo de configuração (assumindo que está no mesmo diretório do script)
CONFIG_JSON = "dimensoes.json"

# ==== LOGS ====
def log_b(t): print(f"\n🚩 ====> {t}\n")
def log_i(t): print(f"ℹ️  {t}")
def log_ok(t): print(f"✅ {t}")
def log_w(t): print(f"⚠️  {t}")
def log_e(t): print(f"❌ {t}")

# ==== Spark ====
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

def spark_session():
    """Cria e configura a sessão Spark para rodar na AWS."""
    # Recomenda-se passar configurações de recursos (memória, cores) via spark-submit
    return (
        SparkSession.builder
        .appName("Dimensoes_EJUD_AWS")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.shuffle.partitions", "64")
        # --- Configurações Essenciais para o S3A Committer ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

def apply_tz_conversion(df, cfg):
    """Aplica conversão de fuso horário para colunas de timestamp."""
    tz = "America/Sao_Paulo"
    local_cfg = set([c.lower() for c in cfg.get("timestamps_local", [])])

    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType) and field.name.lower() not in local_cfg:
            df = df.withColumn(field.name, F.from_utc_timestamp(F.col(field.name), tz))
    return df

def data_path(layer, schema, tabela):
    """Constrói o caminho completo S3 para um arquivo Parquet."""
    bucket = BUCKET_OURO if layer == "ouro" else BUCKET_PRATA if layer == "prata" else BUCKET_BRONZE
    prefix = OURO_PREFIX if layer == "ouro" else PRATA_PREFIX if layer == "prata" else BRONZE_PREFIX
    return f"s3a://{bucket}/{prefix}/{schema}/{tabela}"

# ==== Utils ====
def load_config():
    """Carrega o arquivo de configuração JSON."""
    with open(CONFIG_JSON, "r") as f:
        return json.load(f)

def rewrite_sql(sql_text: str, schemas: set) -> str:
    """Reescreve 'schema.tabela' para 'schema_tabela' para funcionar com TempViews."""
    novo = sql_text
    for s in sorted(schemas, key=len, reverse=True):
        novo = novo.replace(f"{s}.", f"{s}_").replace(f"{s.upper()}.", f"{s.upper()}_")
    return novo

def registra_views(spark, fontes):
    """Lê os Parquets do S3 e os registra como TempViews para uso nas queries."""
    for item in fontes:
        path_schema = item["schema"]
        tabela = item["tabela"]
        alias = item.get("view", tabela)

        if "/" in path_schema:
            layer, schema_name = path_schema.split("/", 1)
        else:
            layer, schema_name = "bronze", path_schema

        path = data_path(layer, schema_name, tabela)

        try:
            log_i(f"-> Registrando View da fonte: {path}")
            # --- CORREÇÃO AQUI ---
            df = spark.read.option("mergeSchema", "true").option("pathGlobFilter", "*.parquet").parquet(path)

            df.createOrReplaceTempView(tabela)
            df.createOrReplaceTempView(f"{schema_name}_{tabela}")

            if alias != tabela:
                df.createOrReplaceTempView(alias)
                df.createOrReplaceTempView(f"{schema_name}_{alias}")

        except Exception as e:
            log_e(f"Erro ao carregar a fonte {path}: {e}")
            raise e

def exec_sql(spark, sql_path: str, fontes):
    """Lê um arquivo SQL, registra as views necessárias e executa a query."""
    with open(sql_path, "r") as f:
        sql = f.read()

    schema_set = {f["schema"].split('/')[-1] for f in fontes}
    registra_views(spark, fontes)

    sql_exec = rewrite_sql(sql, schema_set)
    return spark.sql(sql_exec)

def write_single_parquet_s3(spark, df, dim_nome, camada):
    """Salva o DataFrame como um único arquivo Parquet no S3."""
    bucket = BUCKET_OURO if camada == "ouro" else BUCKET_PRATA
    prefix = OURO_PREFIX if camada == "ouro" else PRATA_PREFIX

    out_file_s3 = f"s3a://{bucket}/{prefix}/{dim_nome}/{dim_nome}.parquet"
    tmp_dir_s3 = f"s3a://{bucket}/{prefix}/_tmp_{dim_nome}"

    log_i(f"Gravando Parquet temporário em: {tmp_dir_s3}")
    df.coalesce(1).write.mode("overwrite").parquet(tmp_dir_s3)

    # --- CORREÇÃO FINAL AQUI ---
    log_i("Movendo arquivo final no S3...")
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    tmp_path = Path(tmp_dir_s3)
    final_path = Path(out_file_s3)

    # Pega o sistema de arquivos CORRETO a partir do próprio caminho S3
    fs = tmp_path.getFileSystem(hconf)

    # Deleta o arquivo final se ele já existir
    if fs.exists(final_path):
        fs.delete(final_path, True)

    # Encontra o arquivo part-* no diretório temporário
    part_file_status = fs.listStatus(tmp_path)
    part_file = [f.getPath() for f in part_file_status if f.getPath().getName().startswith("part-")][0]

    # Cria o diretório pai e renomeia o arquivo
    fs.mkdirs(final_path.getParent())
    fs.rename(part_file, final_path)

    # Limpa o diretório temporário
    fs.delete(tmp_path, True)
    # --- FIM DA CORREÇÃO ---

    log_ok(f"Parquet único salvo no S3: {out_file_s3}")
    return out_file_s3

def load_to_postgres(spark_df, destino_schema, destino_tabela):
    """Carrega um DataFrame Spark em uma tabela do Postgres usando JDBC."""
    if not all([PG_JDBC_URL, PG_USER, PG_PASSWORD]):
        log_w("Variáveis de ambiente do Postgres (PG_JDBC_URL, PG_USER, PG_PASSWORD) não definidas. Carga no DB ignorada.")
        return

    log_b(f"Iniciando carga JDBC para -> {destino_schema}.{destino_tabela}")

    (spark_df.write
     .format("jdbc")
     .option("url", PG_JDBC_URL)
     .option("dbtable", f"{destino_schema}.{destino_tabela}")
     .option("user", PG_USER)
     .option("password", PG_PASSWORD)
     .option("driver", "org.postgresql.Driver")
     .mode("overwrite") # TRUNCATE + INSERT
     .save())

    log_ok(f"Carga JDBC concluída para {destino_schema}.{destino_tabela}")

def process_dim(spark, cfg, dim):
    """Orquestra o processo completo para uma única dimensão."""
    nome = dim["nome"]
    schema_dest = dim["destino"]["schema"]
    tabela_dest = dim["destino"]["tabela"]
    id_projeto = 3  # ID do projeto para auditoria

    log_b(f"Processando dimensão: {nome}")
    dt_ref = datetime.now()

    # Módulo de auditoria (opcional)
    try:
        registrar_inicio_carga(
            id_projeto=id_projeto, schema=schema_dest, tabela=nome,
            dt_referencia=dt_ref, home_script_executor="AWS Spark Job"
        )
    except Exception as e:
        log_w(f"[AUDITORIA] registrar_inicio_carga indisponível — seguindo sem auditoria. Erro: {e}")

    try:
        df = exec_sql(spark, dim["sql"], dim["fontes"])
        df = df.select([F.col(c).alias(c.upper()) for c in df.columns])

        # Renomeios, defaults, casts
        for old, new in dim.get("renomeios", {}).items():
            df = df.withColumnRenamed(old.upper(), new.upper())
        for colname, value in dim.get("defaults", {}).items():
            df = df.withColumn(colname.upper(), F.coalesce(F.col(colname.upper()), F.lit(value)))
        for colname, spark_type in dim.get("casts", {}).items():
            df = df.withColumn(colname.upper(), F.col(colname.upper()).cast(spark_type))

        # Conversão de Timezone
        df = apply_tz_conversion(df, dim)

        # Lógica de dedicação
        pk = [p.upper() for p in dim["pk"]]
        order_specs = []
        for spec in dim.get("ordenacao_pk", []):
            coln, mode = (spec.split(":") + ["desc"])[:2]
            order_func = getattr(F.col(coln.upper()), mode, F.col(coln.upper()).desc)
            order_specs.append(order_func())

        w = Window.partitionBy(*pk).orderBy(*order_specs) if order_specs else Window.partitionBy(*pk).orderBy(F.lit(1))
        df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

        # Salva o resultado no S3
        camada = dim["camada"]
        write_single_parquet_s3(spark, df, nome, camada)

        # Carrega no Postgres (se habilitado)
        if LOAD_TO_DB and dim.get("destino"):
            load_to_postgres(df, schema_dest, tabela_dest)

        # Módulo de auditoria
        try:
            registrar_fim_carga(
                id_projeto=id_projeto, schema=schema_dest, tabela=nome, dt_referencia=dt_ref,
                dt_fim_carga=datetime.now(), qt_registros=df.count(), status="SUCESSO",
                mensagem_erro=None
            )
        except Exception as e:
            log_w(f"[AUDITORIA] registrar_fim_carga indisponível. Erro: {e}")

        log_ok(f"[{nome}] concluída.")

    except Exception as e:
        log_e(f"[{nome}] FALHOU: {e}")
        try:
            registrar_fim_carga(
                id_projeto=id_projeto, schema=schema_dest, tabela=nome, dt_referencia=dt_ref,
                dt_fim_carga=datetime.now(), status="ERRO", mensagem_erro=str(e)
            )
        except Exception as audit_e:
            log_e(f"[AUDITORIA] Falha ao registrar erro: {audit_e}")
        raise

def main(selected_dims=None):
    """Função principal que executa o pipeline para as dimensões selecionadas."""
    inicio = datetime.now()
    log_b("Processo de Dimensões EJUD - Ambiente: AWS S3")

    cfg = load_config()
    todas = {d["nome"] for d in cfg["dimensoes"]}

    if selected_dims:
        dims_plan = [d for d in cfg["dimensoes"] if d["nome"] in selected_dims]
        log_i(f"Executando dimensões selecionadas: {', '.join([d['nome'] for d in dims_plan])}")
    else:
        dims_plan = cfg["dimensoes"]
        log_i("Executando todas as dimensões.")

    spark = spark_session()

    for dim in dims_plan:
        process_dim(spark, cfg, dim)

    log_ok(f"Processo concluído em {datetime.now() - inicio}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executor de dimensões EJUD na AWS")
    parser.add_argument(
        "--dims",
        help="Nomes das dimensões a executar, separados por vírgula. Se omitido, executa todas.",
        default=None
    )
    args = parser.parse_args()
    selected = set(d.strip() for d in args.dims.split(",")) if args.dims else None

    main(selected_dims=selected)
 
