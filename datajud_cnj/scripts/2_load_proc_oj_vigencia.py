import argparse
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def configurar_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


logger = configurar_logger()


def iniciar_spark(app_name: str = "load_proc_oj_vigencia") -> SparkSession:
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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession pronta.")
    return spark


def iceberg_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    try:
        spark.table(full_table_name).limit(1).collect()
        return True
    except Exception:
        return False


def _require_columns(df: DataFrame, table_name: str, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Tabela {table_name} sem colunas obrigatorias: {missing}")


def carregar_base_proc_oj(
    spark: SparkSession,
    source_table: str,
    dim_serventia_table: str,
) -> DataFrame:
    src = spark.table(source_table)
    _require_columns(
        src,
        source_table,
        ["nr_processo", "cd_sist_origem", "cd_seq_tb_origem", "cd_oj_tj", "dth_incl_oj"],
    )

    if "cd_oj_cnj" in src.columns:
        return src.select(
            F.col("nr_processo").cast("decimal(20,0)").alias("nr_processo"),
            F.col("cd_sist_origem").cast("int").alias("cd_sist_origem"),
            F.col("cd_seq_tb_origem").cast("string").alias("cd_seq_tb_origem"),
            F.col("cd_oj_tj").cast("int").alias("cd_oj_tj"),
            F.col("cd_oj_cnj").cast("int").alias("cd_oj_cnj"),
            F.col("dth_incl_oj").cast("timestamp").alias("dt_ini_vig_oj"),
        )

    base = src.select(
        F.col("nr_processo").cast("decimal(20,0)").alias("nr_processo"),
        F.col("cd_sist_origem").cast("int").alias("cd_sist_origem"),
        F.col("cd_seq_tb_origem").cast("string").alias("cd_seq_tb_origem"),
        F.col("cd_oj_tj").cast("int").alias("cd_oj_tj"),
        F.col("dth_incl_oj").cast("timestamp").alias("dt_ini_vig_oj"),
    )

    dim = spark.table(dim_serventia_table).select(
        F.col("cd_oj_tj").cast("int").alias("_dim_cd_oj_tj"),
        F.col("cd_oj_cnj").cast("int").alias("cd_oj_cnj"),
    )

    return (
        base.join(dim, base.cd_oj_tj == dim._dim_cd_oj_tj, "left")
        .drop("_dim_cd_oj_tj")
    )


def filtrar_movimentos_mes(df: DataFrame, source_table_df: DataFrame, ano: int, mes: int) -> DataFrame:
    if "ano" in source_table_df.columns and "mes" in source_table_df.columns:
        return df.join(
            source_table_df
            .where((F.col("ano") == int(ano)) & (F.col("mes") == int(mes)))
            .select(
                F.col("nr_processo").cast("decimal(20,0)").alias("_nr_processo"),
                F.col("cd_sist_origem").cast("int").alias("_cd_sist_origem"),
                F.col("cd_seq_tb_origem").cast("string").alias("_cd_seq_tb_origem"),
            )
            .dropDuplicates(),
            (df.nr_processo == F.col("_nr_processo"))
            & (df.cd_sist_origem == F.col("_cd_sist_origem"))
            & (df.cd_seq_tb_origem == F.col("_cd_seq_tb_origem")),
            "inner",
        ).drop("_nr_processo", "_cd_sist_origem", "_cd_seq_tb_origem")

    anomes = f"{ano}{mes:02d}"
    return df.where(F.date_format("dt_ini_vig_oj", "yyyyMM") == anomes)


def gerar_vigencia(base: DataFrame) -> DataFrame:
    base_limpa = (
        base.where(
            F.col("nr_processo").isNotNull()
            & F.col("cd_sist_origem").isNotNull()
            & F.col("cd_oj_tj").isNotNull()
            & F.col("dt_ini_vig_oj").isNotNull()
        )
        .select(
            "nr_processo",
            "cd_sist_origem",
            "cd_seq_tb_origem",
            "cd_oj_tj",
            "cd_oj_cnj",
            "dt_ini_vig_oj",
        )
        .dropDuplicates()
    )

    w_ordem = Window.partitionBy("nr_processo", "cd_sist_origem").orderBy(
        F.col("dt_ini_vig_oj").asc(),
        F.col("cd_seq_tb_origem").asc_nulls_last(),
        F.col("cd_oj_tj").asc(),
    )

    com_anterior = (
        base_limpa
        .withColumn("_cd_oj_tj_ant", F.lag("cd_oj_tj").over(w_ordem))
        .withColumn("_cd_oj_cnj_ant", F.lag("cd_oj_cnj").over(w_ordem))
    )

    mudancas = com_anterior.where(
        F.col("_cd_oj_tj_ant").isNull()
        | (~F.col("cd_oj_tj").eqNullSafe(F.col("_cd_oj_tj_ant")))
        | (~F.col("cd_oj_cnj").eqNullSafe(F.col("_cd_oj_cnj_ant")))
    ).drop("_cd_oj_tj_ant", "_cd_oj_cnj_ant")

    w_vigencia = Window.partitionBy("nr_processo", "cd_sist_origem").orderBy(
        F.col("dt_ini_vig_oj").asc(),
        F.col("cd_seq_tb_origem").asc_nulls_last(),
        F.col("cd_oj_tj").asc(),
    )

    return mudancas.withColumn(
        "dt_fim_vig_oj",
        F.lead("dt_ini_vig_oj").over(w_vigencia),
    ).withColumn(
        "ano",
        F.year("dt_ini_vig_oj").cast("int"),
    ).withColumn(
        "mes",
        F.month("dt_ini_vig_oj").cast("int"),
    ).select(
        "nr_processo",
        "cd_sist_origem",
        "cd_seq_tb_origem",
        "cd_oj_tj",
        "cd_oj_cnj",
        "dt_ini_vig_oj",
        "dt_fim_vig_oj",
        "ano",
        "mes",
    )


def salvar_full(
    df: DataFrame,
    target_table: str,
    target_exists: bool,
    num_partitions: int,
) -> None:
    df_out = df.repartition(num_partitions, "ano", "mes", "cd_sist_origem")

    if not target_exists:
        logger.info(f"Criando tabela Iceberg {target_table}...")
        (
            df_out.writeTo(target_table)
            .partitionedBy("ano", "mes", "cd_sist_origem")
            .tableProperty("format-version", "2")
            .create()
        )
        return

    logger.info(f"Atualizando full por overwritePartitions em {target_table}...")
    df_out.writeTo(target_table).overwritePartitions()


def substituir_processos_afetados(
    spark: SparkSession,
    target_table: str,
    affected_keys: DataFrame,
    vigencia_afetados: DataFrame,
    num_partitions: int,
) -> None:
    affected_keys.createOrReplaceTempView("_proc_oj_vigencia_affected_keys")

    logger.info("Removendo vigencias antigas dos processos afetados...")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING _proc_oj_vigencia_affected_keys k
           ON t.nr_processo = k.nr_processo
          AND t.cd_sist_origem = k.cd_sist_origem
        WHEN MATCHED THEN DELETE
        """
    )

    logger.info("Inserindo vigencias recalculadas dos processos afetados...")
    (
        vigencia_afetados
        .repartition(num_partitions, "ano", "mes", "cd_sist_origem")
        .writeTo(target_table)
        .append()
    )


def processar_full(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    dim_serventia_table: str,
    num_partitions: int,
) -> None:
    base = carregar_base_proc_oj(spark, source_table, dim_serventia_table)
    vigencia = gerar_vigencia(base)
    salvar_full(vigencia, target_table, iceberg_table_exists(spark, target_table), num_partitions)


def processar_incremental(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    dim_serventia_table: str,
    ano: int,
    mes: int,
    num_partitions: int,
) -> None:
    if not iceberg_table_exists(spark, target_table):
        logger.warning(f"{target_table} nao existe. Executando carga full inicial.")
        processar_full(spark, source_table, target_table, dim_serventia_table, num_partitions)
        return

    src_raw = spark.table(source_table)
    base = carregar_base_proc_oj(spark, source_table, dim_serventia_table)
    movimentos_mes = filtrar_movimentos_mes(base, src_raw, ano, mes)

    affected_keys = movimentos_mes.select("nr_processo", "cd_sist_origem").dropDuplicates()
    total_afetados = affected_keys.count()
    logger.info(f"Processos afetados em {ano}{mes:02d}: {total_afetados}")

    if total_afetados == 0:
        logger.info("Nenhuma distribuicao/redistribuicao nova para atualizar.")
        return

    historico_afetados = base.join(affected_keys, ["nr_processo", "cd_sist_origem"], "inner")
    vigencia_afetados = gerar_vigencia(historico_afetados)

    substituir_processos_afetados(
        spark=spark,
        target_table=target_table,
        affected_keys=affected_keys,
        vigencia_afetados=vigencia_afetados,
        num_partitions=num_partitions,
    )


def parse_anomes(anomes: Optional[str]) -> tuple[int, int]:
    if anomes:
        dt = datetime.strptime(anomes, "%Y%m")
    else:
        dt = datetime.now()
    return dt.year, dt.month


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera e atualiza tb_datajud_processo_oj_vigencia a partir de tb_datajud_processo_oj."
    )
    parser.add_argument(
        "--modo",
        choices=["full", "incremental"],
        default="incremental",
        help="full recalcula toda a tabela; incremental recalcula apenas processos afetados no mes.",
    )
    parser.add_argument("--anomes", type=str, help="Mes de referencia YYYYMM para modo incremental.")
    parser.add_argument("--schema-origem", type=str, default="dw_1inst_prata")
    parser.add_argument("--schema-destino", type=str, default="dw_1inst_prata")
    parser.add_argument("--source-table", type=str, default=None)
    parser.add_argument("--target-table", type=str, default=None)
    parser.add_argument("--dim-serventia-table", type=str, default="ouro.tb_dim_serventia")
    parser.add_argument("--num-partitions", type=int, default=30)
    args = parser.parse_args()

    source_table = args.source_table or f"{args.schema_origem}.tb_datajud_processo_oj"
    target_table = args.target_table or f"{args.schema_destino}.tb_datajud_processo_oj_vigencia"

    spark = iniciar_spark("datajud_proc_oj_vigencia")

    try:
        logger.info(f"Source table: {source_table}")
        logger.info(f"Target table: {target_table}")
        logger.info(f"Modo: {args.modo}")

        if args.modo == "full":
            processar_full(
                spark=spark,
                source_table=source_table,
                target_table=target_table,
                dim_serventia_table=args.dim_serventia_table,
                num_partitions=args.num_partitions,
            )
        else:
            ano, mes = parse_anomes(args.anomes)
            processar_incremental(
                spark=spark,
                source_table=source_table,
                target_table=target_table,
                dim_serventia_table=args.dim_serventia_table,
                ano=ano,
                mes=mes,
                num_partitions=args.num_partitions,
            )

        logger.info("Processamento concluido.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
