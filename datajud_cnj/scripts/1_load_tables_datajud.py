import logging
import posixpath
from typing import Optional, List, Tuple, Dict, Set
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType
from datetime import datetime
from dateutil.relativedelta import relativedelta
from py4j.protocol import Py4JJavaError
import argparse
import os
import boto3


# ----------------------------------------------------------------------
# Config padrão do align
# ----------------------------------------------------------------------
DROP_EXTRA_COLS_DEFAULT = True   # remove colunas que sumiram do DF (por padrão)
EVOLVE_TYPES_DEFAULT = True      # tenta ALTER COLUMN TYPE (best-effort)


# ----------------------------------------------------------------------
# ORDEM DE EXECUÇÃO DAS TABELAS
# ----------------------------------------------------------------------
# Se preencher essa lista, ela será usada como ordem prioritária.
# Se deixar vazia ([]), será usada a ordem default.
ORDEM_EXECUCAO_CUSTOM: List[str] = [
    # "tb_datajud_processo_evento",
    # "tb_datajud_evento_complemento",
]

# Ordem default do projeto DataJud CNJ
ORDEM_EXECUCAO_DEFAULT: List[str] = [
    "tb_datajud_processo_evento",
    "tb_datajud_evento_complemento",
]


# ----------------------------------------------------------------------
# Logger
# ----------------------------------------------------------------------
def configurar_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


logger = configurar_logger()


# ----------------------------------------------------------------------
# Spark
# ----------------------------------------------------------------------
def iniciar_spark(app_name: str = "load_datajud_cnj_tables") -> SparkSession:
    logger.info("Inicializando SparkSession...")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.executor.memory", "9g")
        .config("spark.executor.memoryOverhead", "2g")
        .config("spark.executor.cores", "4")
        .config("spark.executor.instances", "10")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.cores", "2")
        .config("spark.sql.shuffle.partitions", "80")
        .config("spark.memory.fraction", "0.8")
        .config("spark.sql.parquet.enableVectorizedWriter", "true")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # s3a
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession pronta.")
    return spark


# ----------------------------------------------------------------------
# Util: join seguro para S3 (não usar os.path.join)
# ----------------------------------------------------------------------
def s3_join(*parts: str) -> str:
    first = parts[0].rstrip("/")
    rest = [p.strip("/") for p in parts[1:]]
    return posixpath.join(first, *rest)


# ----------------------------------------------------------------------
# Mapeamento cd_sist_origem por banco
# ----------------------------------------------------------------------
def cd_sist_origem_por_banco(banco: str) -> int:
    b = banco.lower().strip()
    mapping = {
        "dcp": 1,
        "pje": 2,
        "eproc1g": 3,
    }
    if b not in mapping:
        raise ValueError(f"Banco '{banco}' não mapeado para cd_sist_origem.")
    return mapping[b]


def garantir_cd_sist_origem(df: DataFrame, banco: str) -> DataFrame:
    if "cd_sist_origem" in df.columns:
        return df
    valor = cd_sist_origem_por_banco(banco)
    logger.info(f"Coluna cd_sist_origem não existe no DF. Adicionando via banco='{banco}' => {valor}.")
    return df.withColumn("cd_sist_origem", lit(int(valor)))


# ----------------------------------------------------------------------
# Ordem de execução (constantes no topo)
# ----------------------------------------------------------------------
def obter_ordem_execucao() -> List[str]:
    if ORDEM_EXECUCAO_CUSTOM:
        logger.info("Usando ORDEM_EXECUCAO_CUSTOM definida no script.")
        return [x.strip().replace(".sql", "") for x in ORDEM_EXECUCAO_CUSTOM if x and x.strip()]

    logger.info("Usando ORDEM_EXECUCAO_DEFAULT.")
    return [x.strip().replace(".sql", "") for x in ORDEM_EXECUCAO_DEFAULT if x and x.strip()]


def validar_e_logar_ordem_execucao(
    ordem: List[str],
    arquivos_sql: List[str],
    consultas_dir: str,
) -> List[str]:
    """
    - Loga quais tabelas da ordem foram encontradas
    - Dá WARNING para as que não existem no diretório
    - Retorna a ordem normalizada (sem .sql, sem duplicados, preservando ordem)
    """
    existentes = {
        posixpath.basename(p).replace(".sql", ""): p
        for p in arquivos_sql
    }

    ordem_norm: List[str] = []
    seen: Set[str] = set()
    for t in ordem:
        tt = t.strip().replace(".sql", "")
        if tt and tt not in seen:
            ordem_norm.append(tt)
            seen.add(tt)

    encontrados = [t for t in ordem_norm if t in existentes]
    faltando = [t for t in ordem_norm if t not in existentes]

    logger.info("========== ORDEM DE EXECUÇÃO ==========")
    logger.info(f"Diretório: {consultas_dir}")
    logger.info(f"Ordem configurada (normalizada): {ordem_norm if ordem_norm else '[]'}")
    logger.info(f"Encontradas na ordem: {encontrados if encontrados else '[]'}")
    if faltando:
        logger.warning(f"⚠️ Tabelas na ordem que NÃO existem em {consultas_dir}: {faltando}")
    logger.info("======================================")

    return ordem_norm


def ordenar_sqls_por_ordem(
    sqls: List[Tuple[str, str]],
    ordem_preferencial: List[str],
) -> List[Tuple[str, str]]:
    """
    Ordena:
      1) tabelas presentes em ordem_preferencial (na ordem informada)
      2) demais tabelas em ordem alfabética
    """
    if not ordem_preferencial:
        return sorted(sqls, key=lambda x: posixpath.basename(x[0]).lower())

    rank = {name: i for i, name in enumerate(ordem_preferencial)}

    def _key(item: Tuple[str, str]):
        filepath, _ = item
        tabela = posixpath.basename(filepath).replace(".sql", "")
        if tabela in rank:
            return (0, rank[tabela], tabela.lower())
        return (1, 10**9, tabela.lower())

    return sorted(sqls, key=_key)


# ----------------------------------------------------------------------
# SQL no S3
# ----------------------------------------------------------------------
def listar_arquivos_sql(spark: SparkSession, s3_path: str) -> list[str]:
    logger.info(f"Listando arquivos SQL em: {s3_path}")

    try:
        path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())

        if not fs.exists(path):
            logger.warning(
                f"Diretório de consultas não existe: {s3_path}. "
                f"Nenhuma consulta será executada para este banco."
            )
            return []

        arquivos_sql: list[str] = []
        files_iter = fs.listFiles(path, True)
        while files_iter.hasNext():
            file_status = files_iter.next()
            if file_status.isFile() and str(file_status.getPath()).endswith(".sql"):
                arquivos_sql.append(str(file_status.getPath()))

        return sorted(arquivos_sql)

    except Exception as e:
        logger.warning(
            f"Falha ao acessar diretório de consultas {s3_path}. "
            f"Nenhuma consulta será executada. Detalhe: {e}"
        )
        return []


def carregar_query_s3(spark: SparkSession, filepath: str) -> str:
    logger.info(f"Lendo query: {filepath}")
    linhas = spark.read.text(filepath).collect()
    return "\n".join([linha.value for linha in linhas])


# ----------------------------------------------------------------------
# Iceberg helpers
# ----------------------------------------------------------------------
def iceberg_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    try:
        spark.table(full_table_name).limit(1).collect()
        return True
    except Exception:
        return False


def glue_delete_table(database: str, table: str) -> None:
    region = os.getenv("AWS_REGION", "sa-east-1")
    glue = boto3.client("glue", region_name=region)
    try:
        glue.delete_table(DatabaseName=database, Name=table)
        logger.warning(f"Glue delete-table OK: {database}.{table}")
    except glue.exceptions.EntityNotFoundException:
        logger.warning(f"Glue table não existe: {database}.{table} (ok)")


def _is_broken_iceberg_metadata(err: Exception) -> bool:
    msg = str(err)
    return (
        "org.apache.iceberg.exceptions.NotFoundException" in msg
        or "NoSuchKeyException" in msg
        or "Location does not exist" in msg
        or ".metadata.json" in msg
    )


# ----------------------------------------------------------------------
# ALIGN de schema
# ----------------------------------------------------------------------
def _schema_to_map(schema: StructType) -> Dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in schema.fields}


def align_schema_iceberg(
    spark: SparkSession,
    full_table_name: str,
    df: DataFrame,
    protected_cols: Set[str],
    drop_extra_cols: bool = DROP_EXTRA_COLS_DEFAULT,
    evolve_types: bool = EVOLVE_TYPES_DEFAULT,
) -> None:
    """
    Alinha a tabela Iceberg ao schema do DF:
      - ADD COLUMN: colunas novas
      - DROP COLUMN: colunas que sumiram do DF (se drop_extra_cols=True)
      - ALTER COLUMN TYPE: tenta alterar tipo (best-effort)
    protected_cols: colunas que NÃO podem ser dropadas (ex.: partições)
    """
    target_schema = spark.table(full_table_name).schema
    src_map = _schema_to_map(df.schema)
    tgt_map = _schema_to_map(target_schema)

    src_cols = set(src_map.keys())
    tgt_cols = set(tgt_map.keys())

    to_add = sorted(list(src_cols - tgt_cols))
    to_drop_all = sorted(list(tgt_cols - src_cols))
    to_drop = [c for c in to_drop_all if (drop_extra_cols and c not in protected_cols)]
    type_changes = sorted([c for c in (src_cols & tgt_cols) if src_map[c] != tgt_map[c]])

    if not to_add and not to_drop and not type_changes:
        logger.info(f"[ALIGN] {full_table_name}: schema já alinhado.")
        return

    logger.info(
        f"[ALIGN] {full_table_name}: "
        f"{len(to_add)} add / {len(to_drop)} drop / {len(type_changes)} type change "
        f"(protegidas: {sorted(list(protected_cols))})"
    )

    # ADD
    for c in to_add:
        col_type = src_map[c]
        logger.info(f"[ALIGN][ADD] {c} {col_type}")
        spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMN `{c}` {col_type}")

    # ALTER TYPE (best-effort)
    if evolve_types:
        for c in type_changes:
            src_t = src_map[c]
            tgt_t = tgt_map[c]
            try:
                logger.info(f"[ALIGN][TYPE] `{c}`: {tgt_t} -> {src_t}")
                spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN `{c}` TYPE {src_t}")
            except Exception as e:
                logger.warning(
                    f"[ALIGN][TYPE][SKIP] Não deu pra alterar `{c}` ({tgt_t}->{src_t}): "
                    f"{str(e).splitlines()[0]}"
                )

    # DROP (exceto protegidas)
    if drop_extra_cols and to_drop:
        for c in to_drop:
            logger.info(f"[ALIGN][DROP] {c}")
            spark.sql(f"ALTER TABLE {full_table_name} DROP COLUMN `{c}`")


def salvar_iceberg(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
    partition_cols: List[str],
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    num_partitions: int = 30,
) -> None:
    logger.info(f"Gravando em Iceberg: {full_table_name}")
    logger.info(f"Partition cols: {partition_cols}")

    df_out = df
    if "ano" in partition_cols and ano is not None:
        df_out = df_out.withColumn("ano", lit(int(ano)))
    if "mes" in partition_cols and mes is not None:
        df_out = df_out.withColumn("mes", lit(int(mes)))

    df_out = df_out.repartition(num_partitions)

    try:
        if not iceberg_table_exists(spark, full_table_name):
            logger.info("Tabela não existe. Criando...")
            (
                df_out.writeTo(full_table_name)
                .partitionedBy(*partition_cols)
                .tableProperty("format-version", "2")
                .create()
            )
            logger.info("Tabela criada com sucesso.")
            return

        protected_cols = set(partition_cols)
        try:
            align_schema_iceberg(
                spark=spark,
                full_table_name=full_table_name,
                df=df_out,
                protected_cols=protected_cols,
                drop_extra_cols=DROP_EXTRA_COLS_DEFAULT,
                evolve_types=EVOLVE_TYPES_DEFAULT,
            )
        except Exception as e:
            logger.warning(
                f"[ALIGN][SKIP] Não consegui alinhar schema para {full_table_name}: {str(e).splitlines()[0]}"
            )

        logger.info("Tabela existe. overwritePartitions...")
        df_out.writeTo(full_table_name).overwritePartitions()
        logger.info("Gravação Iceberg concluída.")

    except Py4JJavaError as e:
        if _is_broken_iceberg_metadata(e):
            logger.warning(
                f"Tabela Iceberg aparentemente quebrada (metadata ausente). "
                f"Droppando {full_table_name} e recriando..."
            )
            schema, tbl = full_table_name.split(".", 1)
            glue_delete_table(schema, tbl)

            (
                df_out.writeTo(full_table_name)
                .partitionedBy(*partition_cols)
                .tableProperty("format-version", "2")
                .create()
            )
            logger.info("Tabela recriada com sucesso após drop.")
            return
        raise


# ----------------------------------------------------------------------
# Classificação SQL (mensal vs não mensal)
# ----------------------------------------------------------------------
def _separar_sqls_por_tipo(
    spark: SparkSession,
    arquivos_sql: List[str],
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    mensais: List[Tuple[str, str]] = []
    nao_mensais: List[Tuple[str, str]] = []

    for filepath in arquivos_sql:
        query = carregar_query_s3(spark, filepath)
        if "{anomes}" in query:
            mensais.append((filepath, query))
        else:
            nao_mensais.append((filepath, query))

    return mensais, nao_mensais


def _logar_sqls_classificadas(
    sqls_mensais: List[Tuple[str, str]],
    sqls_nao_mensais: List[Tuple[str, str]],
) -> None:
    nomes_mensais = [posixpath.basename(p).replace(".sql", "") for p, _ in sqls_mensais]
    nomes_nao_mensais = [posixpath.basename(p).replace(".sql", "") for p, _ in sqls_nao_mensais]

    logger.info("========== CLASSIFICAÇÃO DAS CONSULTAS ==========")
    logger.info(f"TOTAL: {len(nomes_mensais) + len(nomes_nao_mensais)}")
    logger.info(f"MENSAIS (contém {{anomes}}): {len(nomes_mensais)}")
    if nomes_mensais:
        logger.info(" - " + ", ".join(sorted(nomes_mensais)))

    logger.info(f"NÃO MENSAIS (não contém {{anomes}}): {len(nomes_nao_mensais)}")
    if nomes_nao_mensais:
        logger.info(" - " + ", ".join(sorted(nomes_nao_mensais)))

    logger.info("===============================================")


# ----------------------------------------------------------------------
# Processamento
# ----------------------------------------------------------------------
def processar_consultas(
    spark: SparkSession,
    banco: str,
    consultas_base_dir: str,
    schema_destino: str,
    start_date: datetime,
    end_date: datetime,
    table: Optional[str] = None,
    grupo: Optional[str] = None,
) -> None:
    grupo_norm = (grupo or "").strip().strip("/").replace("\\", "/")
    consultas_dir = s3_join(consultas_base_dir, banco, grupo_norm) if grupo_norm else s3_join(consultas_base_dir, banco)
    if grupo_norm:
        logger.info(f"Filtro de grupo ativado: {grupo_norm}")

    arquivos_sql = listar_arquivos_sql(spark, consultas_dir)

    if not arquivos_sql:
        logger.warning(f"Nenhum .sql encontrado em {consultas_dir}")
        return

    # single-table
    if table:
        alvo = f"{table.replace('.sql','')}.sql"
        arquivos_sql = [p for p in arquivos_sql if posixpath.basename(p) == alvo]
        if not arquivos_sql:
            logger.warning(f"Não encontrei o arquivo '{alvo}' em {consultas_dir}")
            return
        logger.info(f"Modo single-table ativado: executando apenas '{alvo}'")

    # ordem via constantes (com validação + logs)
    ordem_exec = obter_ordem_execucao()
    ordem_exec = validar_e_logar_ordem_execucao(
        ordem=ordem_exec,
        arquivos_sql=arquivos_sql,
        consultas_dir=consultas_dir,
    )

    # classifica e ordena
    sqls_mensais, sqls_nao_mensais = _separar_sqls_por_tipo(spark, arquivos_sql)
    sqls_nao_mensais = ordenar_sqls_por_ordem(sqls_nao_mensais, ordem_exec)
    sqls_mensais = ordenar_sqls_por_ordem(sqls_mensais, ordem_exec)

    _logar_sqls_classificadas(sqls_mensais, sqls_nao_mensais)

    # ---------------- NÃO MENSAIS ----------------
    if sqls_nao_mensais:
        logger.info("========== EXECUÇÃO NÃO MENSAL (1x por run) ==========")
        for filepath, query in sqls_nao_mensais:
            tabela = posixpath.basename(filepath).replace(".sql", "")
            full_table_name = f"{schema_destino}.{tabela}"

            try:
                logger.info(f"[NÃO MENSAL] Executando: {tabela} | Partição: (cd_sist_origem)")
                df = spark.sql(query)
                df = garantir_cd_sist_origem(df, banco)

                salvar_iceberg(
                    spark=spark,
                    df=df,
                    full_table_name=full_table_name,
                    partition_cols=["cd_sist_origem"],
                    ano=None,
                    mes=None,
                )
            except Exception as e:
                logger.error(f"Erro ao processar (não mensal) {filepath}: {e}", exc_info=True)

    # ---------------- MENSAIS ----------------
    if sqls_mensais:
        logger.info("========== EXECUÇÃO MENSAL (por anomes) ==========")
        current_date = start_date
        while current_date <= end_date:
            ano, mes = current_date.year, current_date.month
            anomes = f"{ano}{mes:02d}"

            logger.info(f"================= Período {anomes} =================")

            for filepath, query in sqls_mensais:
                tabela = posixpath.basename(filepath).replace(".sql", "")
                full_table_name = f"{schema_destino}.{tabela}"

                try:
                    logger.info(
                        f"[MENSAL] Executando: {tabela} | anomes={anomes} | Partição: (ano, mes, cd_sist_origem)"
                    )
                    df = spark.sql(query.format(anomes=anomes))
                    df = garantir_cd_sist_origem(df, banco)

                    salvar_iceberg(
                        spark=spark,
                        df=df,
                        full_table_name=full_table_name,
                        partition_cols=["ano", "mes", "cd_sist_origem"],
                        ano=ano,
                        mes=mes,
                    )
                except Exception as e:
                    logger.error(f"Erro ao processar (mensal) {filepath} para {anomes}: {e}", exc_info=True)

            current_date += relativedelta(months=1)

    logger.info("Processamento finalizado.")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Carga DataJud CNJ (SQL no S3 -> Iceberg).")
    parser.add_argument("--banco", required=True, help="Nome do banco (ex: pje, eproc1g, dcp)")
    parser.add_argument("--anomes-inicio", type=str, help="Ano/mês inicial YYYYMM")
    parser.add_argument("--anomes-fim", type=str, help="Ano/mês final YYYYMM")
    parser.add_argument("--table", type=str, help="Nome do SQL (sem .sql) para rodar apenas uma tabela")
    parser.add_argument(
        "--grupo",
        type=str,
        help="Grupo/subdiretório de consultas dentro do banco (ex: processo, eventos, partes_pessoas, documentos)",
    )
    parser.add_argument(
        "--schema-destino",
        type=str,
        default="dw_1inst_prata",
        help="Schema destino Iceberg (default: dw_1inst_prata)",
    )
    args = parser.parse_args()

    banco = args.banco.lower().strip()
    schema_destino = (args.schema_destino or "dw_1inst_prata").strip()

    logger.info(f"Iniciando para banco: {banco}")
    logger.info(f"Schema destino: {schema_destino}")

    # DataJud CNJ
    consultas_base_dir = "s3a://sgdai-projetos/datajud_cnj/consultas"

    spark = iniciar_spark(f"{banco}_datajud_cnj_iceberg")

    try:
        # ✅ Sem temp views: suas consultas já usam Iceberg direto

        # janela de datas
        if args.anomes_inicio or args.anomes_fim:
            anomes_inicio = args.anomes_inicio or args.anomes_fim
            anomes_fim = args.anomes_fim or args.anomes_inicio

            start_date = datetime.strptime(anomes_inicio, "%Y%m")
            end_date = datetime.strptime(anomes_fim, "%Y%m")

            if end_date < start_date:
                start_date, end_date = end_date, start_date

            logger.info(f"🛠️ Janela mensal informada: {start_date.strftime('%Y-%m')} até {end_date.strftime('%Y-%m')}")
        else:
            hoje = datetime.now()
            primeiro_dia = datetime(hoje.year, hoje.month, 1)
            start_date = primeiro_dia - relativedelta(months=1)
            end_date = hoje
            logger.info(f"📅 Janela mensal padrão: {start_date.strftime('%Y-%m')} até {end_date.strftime('%Y-%m')}")

        processar_consultas(
            spark=spark,
            banco=banco,
            consultas_base_dir=consultas_base_dir,
            schema_destino=schema_destino,
            start_date=start_date,
            end_date=end_date,
            table=args.table,
            grupo=args.grupo,
        )

        logger.info("🏁 Processo concluído.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
