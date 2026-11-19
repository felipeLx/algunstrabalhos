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
# Config padrão do align (igual ao outro script)
# ----------------------------------------------------------------------
DROP_EXTRA_COLS_DEFAULT = True   # remove colunas que sumiram do DF (por padrão)
EVOLVE_TYPES_DEFAULT = True      # tenta ALTER COLUMN TYPE (best-effort)


# ------------------------------------------------------------------------------
# Logger
# ------------------------------------------------------------------------------
def configurar_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


logger = configurar_logger()


# ------------------------------------------------------------------------------
# Spark
# ------------------------------------------------------------------------------
def iniciar_spark(app_name: str = "load_ae_decl_acervo_apex_tables") -> SparkSession:
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


# ------------------------------------------------------------------------------
# Util: join seguro para S3 (não usar os.path.join)
# ------------------------------------------------------------------------------
def s3_join(*parts: str) -> str:
    first = parts[0].rstrip("/")
    rest = [p.strip("/") for p in parts[1:]]
    return posixpath.join(first, *rest)


# ------------------------------------------------------------------------------
# Mapeamento cd_sist_origem por banco
# ------------------------------------------------------------------------------
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


def garantir_cd_sist_origem(df: DataFrame, banco: Optional[str], inferir: bool = True) -> DataFrame:
    if "cd_sist_origem" in df.columns:
        return df

    if not inferir:
        logger.info("Coluna cd_sist_origem não existe no DF e a inferência foi desabilitada.")
        return df

    if not banco:
        raise ValueError("Não é possível inferir cd_sist_origem sem informar --banco.")

    valor = cd_sist_origem_por_banco(banco)
    logger.info(f"Coluna cd_sist_origem não existe no DF. Adicionando via banco='{banco}' => {valor}.")
    return df.withColumn("cd_sist_origem", lit(int(valor)))


# ------------------------------------------------------------------------------
# SQL no S3
# ------------------------------------------------------------------------------
def listar_arquivos_sql(spark: SparkSession, s3_path: str) -> list[str]:
    logger.info(f"Listando arquivos SQL em: {s3_path}")

    try:
        path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())

        # 🔎 Se o diretório não existir, apenas avisa e retorna vazio
        if not fs.exists(path):
            logger.warning(
                f"Diretório de consultas não existe: {s3_path}. "
                f"Nenhuma consulta será executada para este diretório."
            )
            return []

        arquivos = [
            str(f.getPath())
            for f in fs.listStatus(path)
            if f.isFile() and str(f.getPath()).endswith(".sql")
        ]

        return arquivos

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


# ------------------------------------------------------------------------------
# Iceberg
# ------------------------------------------------------------------------------
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


# ------------------------------------------------------------------------------
# ALIGN de schema (mesmo conceito do outro script)
# ------------------------------------------------------------------------------
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
                logger.warning(f"[ALIGN][TYPE][SKIP] Não deu pra alterar `{c}` ({tgt_t}->{src_t}): {str(e).splitlines()[0]}")

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
    write_mode: str = "overwrite",
    ano: Optional[int] = None,
    mes: Optional[int] = None,
    num_partitions: int = 30,
) -> None:
    logger.info(f"Gravando em Iceberg: {full_table_name}")
    logger.info(f"Partition cols: {partition_cols}")
    logger.info(f"Write mode: {write_mode}")

    df_out = df
    if "ano" in partition_cols and ano is not None:
        df_out = df_out.withColumn("ano", lit(int(ano)))
    if "mes" in partition_cols and mes is not None:
        df_out = df_out.withColumn("mes", lit(int(mes)))

    df_out = df_out.repartition(num_partitions)

    try:
        if not iceberg_table_exists(spark, full_table_name):
            logger.info("Tabela não existe. Criando...")
            writer = (
                df_out.writeTo(full_table_name)
                .tableProperty("format-version", "2")
            )

            if partition_cols:
                writer = writer.partitionedBy(*partition_cols)

            writer.create()
            logger.info("Tabela criada com sucesso.")
            return

        protected_cols = set(partition_cols)
        drop_extra_cols = DROP_EXTRA_COLS_DEFAULT if write_mode == "overwrite" else False
        try:
            align_schema_iceberg(
                spark=spark,
                full_table_name=full_table_name,
                df=df_out,
                protected_cols=protected_cols,
                drop_extra_cols=drop_extra_cols,
                evolve_types=EVOLVE_TYPES_DEFAULT,
            )
        except Exception as e:
            logger.warning(f"[ALIGN][SKIP] Não consegui alinhar schema para {full_table_name}: {str(e).splitlines()[0]}")

        if write_mode == "append":
            logger.info("Tabela existe. append...")
            df_out.writeTo(full_table_name).append()
        elif partition_cols:
            logger.info("Tabela existe. overwritePartitions...")
            df_out.writeTo(full_table_name).overwritePartitions()
        else:
            logger.info("Tabela existe sem partição. overwrite...")
            df_out.writeTo(full_table_name).overwrite()
        logger.info("Gravação Iceberg concluída.")

    except Py4JJavaError as e:
        if _is_broken_iceberg_metadata(e):
            if write_mode == "append":
                raise RuntimeError(
                    f"Falha de metadata Iceberg em modo append para {full_table_name}. "
                    "Abortando para evitar qualquer recriacao destrutiva da tabela historica."
                ) from e
            logger.warning(
                f"Tabela Iceberg aparentemente quebrada (metadata ausente). "
                f"Droppando {full_table_name} e recriando..."
            )
            schema, tbl = full_table_name.split(".", 1)
            glue_delete_table(schema, tbl)

            writer = (
                df_out.writeTo(full_table_name)
                .tableProperty("format-version", "2")
            )

            if partition_cols:
                writer = writer.partitionedBy(*partition_cols)

            writer.create()
            logger.info("Tabela recriada com sucesso após drop.")
            return

        raise


# ------------------------------------------------------------------------------
# Processamento
# ------------------------------------------------------------------------------
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


def processar_consultas(
    spark: SparkSession,
    banco: Optional[str],
    consultas_base_dir: str,
    schema_destino: str,
    start_date: datetime,
    end_date: datetime,
    table: Optional[str] = None,
    infer_cd_sist_origem: bool = True,
    partition_cols: Optional[List[str]] = None,
    write_mode: str = "overwrite",
) -> None:
    consultas_dir = s3_join(consultas_base_dir, banco) if banco else consultas_base_dir
    partition_cols = partition_cols or []
    arquivos_sql = listar_arquivos_sql(spark, consultas_dir)

    if not arquivos_sql:
        msg = f"Nenhum .sql encontrado em {consultas_dir}"
        if table:
            raise FileNotFoundError(msg)
        logger.warning(msg)
        return

    if table:
        alvo = f"{table.replace('.sql','')}.sql"
        nomes_sql = {posixpath.basename(p) for p in arquivos_sql}
        if alvo not in nomes_sql:
            raise FileNotFoundError(f"Nao encontrei o arquivo '{alvo}' em {consultas_dir}")

    if table:
        alvo = f"{table.replace('.sql','')}.sql"
        arquivos_sql = [p for p in arquivos_sql if posixpath.basename(p) == alvo]
        if not arquivos_sql:
            logger.warning(f"Não encontrei o arquivo '{alvo}' em {consultas_dir}")
            return
        logger.info(f"Modo single-table ativado: executando apenas '{alvo}'")

    sqls_mensais, sqls_nao_mensais = _separar_sqls_por_tipo(spark, arquivos_sql)
    _logar_sqls_classificadas(sqls_mensais, sqls_nao_mensais)

    # ---------------- NÃO MENSAIS ----------------
    if sqls_nao_mensais:
        logger.info("========== EXECUÇÃO NÃO MENSAL (1x por run) ==========")
        for filepath, query in sqls_nao_mensais:
            tabela = posixpath.basename(filepath).replace(".sql", "")
            full_table_name = f"{schema_destino}.{tabela}"

            try:
                logger.info(f"[NÃO MENSAL] Executando: {tabela} | Partições: {partition_cols}")
                df = spark.sql(query)
                df = garantir_cd_sist_origem(df, banco, inferir=infer_cd_sist_origem)

                salvar_iceberg(
                    spark=spark,
                    df=df,
                    full_table_name=full_table_name,
                    partition_cols=partition_cols,
                    write_mode=write_mode,
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
                    monthly_partition_cols = partition_cols.copy()
                    if "ano" not in monthly_partition_cols:
                        monthly_partition_cols.insert(0, "ano")
                    if "mes" not in monthly_partition_cols:
                        monthly_partition_cols.insert(1 if "ano" in monthly_partition_cols else 0, "mes")

                    logger.info(f"[MENSAL] Executando: {tabela} | anomes={anomes} | Partições: {monthly_partition_cols}")
                    df = spark.sql(query.format(anomes=anomes))
                    df = garantir_cd_sist_origem(df, banco, inferir=infer_cd_sist_origem)

                    salvar_iceberg(
                        spark=spark,
                        df=df,
                        full_table_name=full_table_name,
                        partition_cols=monthly_partition_cols,
                        write_mode=write_mode,
                        ano=ano,
                        mes=mes,
                    )
                except Exception as e:
                    logger.error(f"Erro ao processar (mensal) {filepath} para {anomes}: {e}", exc_info=True)

            current_date += relativedelta(months=1)

    logger.info("Processamento finalizado.")


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Carga Indicadores Locais (SQL no S3 -> Iceberg).")
    parser.add_argument("--banco", required=False, help="Nome do banco (ex: pje, eproc1g, dcp)")
    parser.add_argument("--anomes-inicio", type=str, help="Ano/mês inicial YYYYMM")
    parser.add_argument("--anomes-fim", type=str, help="Ano/mês final YYYYMM")
    parser.add_argument("--table", type=str, help="Nome do SQL (sem .sql) para rodar apenas uma tabela")

    parser.add_argument(
        "--schema-destino",
        type=str,
        default="dw_apex",
        help="Schema destino Iceberg (default: dw_apex)",
    )
    parser.add_argument(
        "--consultas-base-dir",
        type=str,
        default="s3a://sgdai-projetos/1inst_ae_declinados_acervo/consultas",
        help="Diretorio base de consultas SQL no S3 (default: projeto atual)",
    )
    parser.add_argument(
    "--infer-cd-sist-origem",
    type=int,
    default=1,
    help="1 = infere cd_sist_origem pelo banco quando a coluna nao existir; 0 = nao inferir",
    )
    parser.add_argument(
        "--partition-cols",
        type=str,
        default="cd_sist_origem",
        help="Colunas de particao separadas por virgula. Ex: cd_sist_origem ou ano,mes,cd_sist_origem. Use none para nao particionar.",
    )
    parser.add_argument(
        "--job-name",
        type=str,
        default="apex_build_dataset",
        help="Nome da aplicação Spark",
    )

    parser.add_argument(
        "--write-mode",
        type=str,
        default="overwrite",
        choices=["overwrite", "append"],
        help="Modo de escrita no Iceberg: overwrite (default) ou append.",
    )

    args = parser.parse_args()

    banco = args.banco.lower().strip() if args.banco else None
    schema_destino = (args.schema_destino or "prata").strip()
    consultas_base_dir = args.consultas_base_dir.strip().rstrip("/")
    infer_cd_sist_origem = int(args.infer_cd_sist_origem) == 1
    raw_partition_cols = (args.partition_cols or "").strip()
    if raw_partition_cols.lower() in {"", "none", "null", "sem_particao", "no_partition"}:
        partition_cols = []
    else:
        partition_cols = [c.strip() for c in raw_partition_cols.split(",") if c.strip()]
    write_mode = args.write_mode.strip().lower()

    logger.info(f"Iniciando para banco: {banco}")
    logger.info(f"Schema destino: {schema_destino}")
    logger.info(f"Consultas base dir: {consultas_base_dir}")
    logger.info(f"Inferir cd_sist_origem: {infer_cd_sist_origem}")
    logger.info(f"Partition cols: {partition_cols}")
    logger.info(f"Write mode: {write_mode}")

    spark = iniciar_spark(args.job_name)

    try:

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
            infer_cd_sist_origem=infer_cd_sist_origem,
            partition_cols=partition_cols,
            write_mode=write_mode,
        )

        logger.info("🏁 Processo concluído.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
