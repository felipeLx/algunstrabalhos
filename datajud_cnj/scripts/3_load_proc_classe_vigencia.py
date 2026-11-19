import argparse
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


APP_NAME = "datajud_proc_classe_vigencia"
DEFAULT_SOURCE_TABLE = "dw_1inst_prata.tb_dcp_processo_evento"
DEFAULT_COMPLEMENT_TABLE = "dw_1inst_prata.tb_dcp_evento_complemento"
DEFAULT_DIM_COMPLEMENT_TABLE = "ouro.tb_dim_cnj_complemento"
DEFAULT_DIM_COMPLEMENT_TAB_TABLE = "ouro.tb_dim_cnj_complemento_tabelado"
DEFAULT_PROCESS_CLASS_TABLE = "dw_1inst_prata.tb_datajud_processo"
DEFAULT_TARGET_TABLE = "dw_1inst_prata.tb_datajud_processo_classe_vigencia"


def configurar_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


logger = configurar_logger()


def iniciar_spark(app_name: str = APP_NAME) -> SparkSession:
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


def require_columns(df: DataFrame, table_name: str, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Tabela {table_name} sem colunas obrigatorias: {missing}")


def carregar_movimentos_classe(
    spark: SparkSession,
    source_table: str,
    complement_table: str,
    dim_complement_table: str,
    dim_complement_tab_table: str,
    cd_sist_origem: int,
) -> DataFrame:
    src = spark.table(source_table)
    require_columns(src, source_table, ["nr_processo", "cd_evento", "cd_tb_origem", "cd_seq_tb_origem", "dth_evento"])

    eventos = src.where(F.col("cd_evento").cast("int").isin(14738, 14739)).select(
        F.col("nr_processo").cast("decimal(20,0)").alias("nr_processo"),
        F.col("cd_tb_origem").cast("int").alias("cd_tb_origem"),
        F.col("cd_seq_tb_origem").cast("string").alias("cd_seq_tb_origem"),
        F.col("cd_evento").cast("int").alias("cd_evento"),
        F.col("dth_evento").cast("timestamp").alias("dth_evento"),
    )

    tec = spark.table(complement_table)
    require_columns(
        tec,
        complement_table,
        ["nr_processo", "cd_tb_origem", "cd_seq_tb_origem", "cd_compl", "cd_compl_tab", "ds_vlr_compl"],
    )

    cp = spark.table(dim_complement_table)
    require_columns(cp, dim_complement_table, ["cd_compl", "ds_compl"])

    ct = spark.table(dim_complement_tab_table)
    require_columns(ct, dim_complement_tab_table, ["cd_compl_tab", "ds_compl_tab"])

    complementos = (
        tec.alias("tec")
        .join(
            cp.alias("cp"),
            F.col("tec.cd_compl").cast("int") == F.col("cp.cd_compl").cast("int"),
            "left",
        )
        .join(
            ct.alias("ct"),
            F.col("tec.cd_compl_tab").cast("string") == F.col("ct.cd_compl_tab").cast("string"),
            "left",
        )
        .select(
            F.col("tec.nr_processo").cast("decimal(20,0)").alias("nr_processo"),
            F.col("tec.cd_tb_origem").cast("int").alias("cd_tb_origem"),
            F.col("tec.cd_seq_tb_origem").cast("string").alias("cd_seq_tb_origem"),
            F.lower(F.col("cp.ds_compl")).alias("ds_compl_norm"),
            F.coalesce(F.col("ct.ds_compl_tab"), F.col("tec.ds_vlr_compl")).cast("string").alias("ds_vlr_compl"),
        )
        .withColumn("_valor_num", F.regexp_extract(F.col("ds_vlr_compl"), r"(\d+)", 1).cast("int"))
    )

    return (
        eventos.join(complementos, ["nr_processo", "cd_tb_origem", "cd_seq_tb_origem"], "left")
        .groupBy("nr_processo", "cd_tb_origem", "cd_seq_tb_origem", "cd_evento", "dth_evento")
        .agg(
            F.max(
                F.when(F.col("ds_compl_norm").contains("anterior"), F.col("_valor_num"))
            ).alias("cd_classe_anterior"),
            F.max(
                F.when(
                    F.col("ds_compl_norm").contains("nova")
                    | F.col("ds_compl_norm").contains("atual"),
                    F.col("_valor_num"),
                )
            ).alias("cd_classe_atual"),
        )
        .withColumn("cd_sist_origem", F.lit(int(cd_sist_origem)).cast("int"))
        .where(F.col("nr_processo").isNotNull())
    )


def carregar_primeira_distribuicao(source_raw: DataFrame, cd_sist_origem: int) -> DataFrame:
    return (
        source_raw
        .where(F.col("cd_evento").cast("int") == 26)
        .groupBy(F.col("nr_processo").cast("decimal(20,0)").alias("nr_processo"))
        .agg(F.min(F.col("dth_evento").cast("timestamp")).alias("dt_distribuicao"))
        .withColumn("cd_sist_origem", F.lit(int(cd_sist_origem)).cast("int"))
        .where(F.col("nr_processo").isNotNull() & F.col("dt_distribuicao").isNotNull())
    )


def carregar_classe_base_processo(
    spark: SparkSession,
    process_class_table: str,
    distribuicoes: DataFrame,
) -> DataFrame:
    proc = spark.table(process_class_table)
    require_columns(proc, process_class_table, ["nr_processo", "cd_classe"])

    return (
        proc.select(
            F.col("nr_processo").cast("decimal(20,0)").alias("nr_processo"),
            F.col("cd_classe").cast("int").alias("cd_classe"),
        )
        .where(F.col("nr_processo").isNotNull() & F.col("cd_classe").isNotNull())
        .join(distribuicoes, ["nr_processo"], "inner")
        .select(
            "nr_processo",
            "cd_sist_origem",
            F.lit(None).cast("string").alias("cd_seq_tb_origem"),
            "cd_classe",
            F.col("dt_distribuicao").alias("dt_ini_vig_classe"),
            F.col("dt_distribuicao").alias("_dt_evento_regra"),
        )
    )


def filtrar_movimentos_mes(movimentos: DataFrame, source_raw: DataFrame, ano: int, mes: int) -> DataFrame:
    if "ano" in source_raw.columns and "mes" in source_raw.columns:
        keys_mes = (
            source_raw
            .where((F.col("ano") == int(ano)) & (F.col("mes") == int(mes)))
            .where(F.col("cd_evento").cast("int").isin(14738, 14739))
            .select(
                F.col("nr_processo").cast("decimal(20,0)").alias("_nr_processo"),
                F.col("cd_tb_origem").cast("int").alias("_cd_tb_origem"),
                F.col("cd_seq_tb_origem").cast("string").alias("_cd_seq_tb_origem"),
            )
            .dropDuplicates()
        )
        return movimentos.join(
            keys_mes,
            (movimentos.nr_processo == F.col("_nr_processo"))
            & (movimentos.cd_tb_origem == F.col("_cd_tb_origem"))
            & (movimentos.cd_seq_tb_origem == F.col("_cd_seq_tb_origem")),
            "inner",
        ).drop("_nr_processo", "_cd_tb_origem", "_cd_seq_tb_origem")

    anomes = f"{ano}{mes:02d}"
    return movimentos.where(F.date_format("dth_evento", "yyyyMM") == anomes)


def gerar_vigencia(
    movimentos: DataFrame,
    distribuicoes: DataFrame,
    classe_base_processo: DataFrame,
) -> DataFrame:
    evolucoes = movimentos.where(
        (F.col("cd_evento") == 14739) & F.col("cd_classe_atual").isNotNull()
    ).select(
        "nr_processo",
        "cd_sist_origem",
        "cd_seq_tb_origem",
        F.col("cd_classe_atual").alias("cd_classe"),
        F.col("dth_evento").alias("dt_ini_vig_classe"),
        F.col("dth_evento").alias("_dt_evento_regra"),
    )

    retificacoes = movimentos.where(
        (F.col("cd_evento") == 14738) & F.col("cd_classe_atual").isNotNull()
    ).select(
        "nr_processo",
        "cd_sist_origem",
        "cd_seq_tb_origem",
        F.col("cd_classe_atual").alias("cd_classe"),
        F.col("dth_evento").alias("dth_retificacao"),
    )

    evolucoes_ref = movimentos.where(
        (F.col("cd_evento") == 14739) & F.col("cd_classe_atual").isNotNull()
    ).select(
        F.col("nr_processo").alias("_nr_processo_evo"),
        F.col("cd_sist_origem").alias("_cd_sist_origem_evo"),
        F.col("dth_evento").alias("_dth_evolucao"),
    )

    retificacoes_com_inicio = (
        retificacoes
        .join(
            evolucoes_ref,
            (retificacoes.nr_processo == F.col("_nr_processo_evo"))
            & (retificacoes.cd_sist_origem == F.col("_cd_sist_origem_evo"))
            & (F.col("_dth_evolucao") < retificacoes.dth_retificacao),
            "left",
        )
        .groupBy(
            "nr_processo",
            "cd_sist_origem",
            "cd_seq_tb_origem",
            "cd_classe",
            "dth_retificacao",
        )
        .agg(F.max("_dth_evolucao").alias("_ultima_evolucao_anterior"))
        .join(distribuicoes, ["nr_processo", "cd_sist_origem"], "left")
        .select(
            "nr_processo",
            "cd_sist_origem",
            "cd_seq_tb_origem",
            "cd_classe",
            F.coalesce(
                F.col("_ultima_evolucao_anterior"),
                F.col("dt_distribuicao"),
                F.col("dth_retificacao"),
            ).alias("dt_ini_vig_classe"),
            F.col("dth_retificacao").alias("_dt_evento_regra"),
        )
    )

    base = (
        classe_base_processo
        .unionByName(evolucoes)
        .unionByName(retificacoes_com_inicio)
        .where(
            F.col("nr_processo").isNotNull()
            & F.col("cd_sist_origem").isNotNull()
            & F.col("cd_classe").isNotNull()
            & F.col("dt_ini_vig_classe").isNotNull()
        )
        .dropDuplicates()
    )

    w_mesmo_inicio = Window.partitionBy(
        "nr_processo",
        "cd_sist_origem",
        "dt_ini_vig_classe",
    ).orderBy(
        F.col("_dt_evento_regra").desc_nulls_last(),
        F.col("cd_seq_tb_origem").desc_nulls_last(),
    )

    base_corrigida = (
        base
        .withColumn("_rn_inicio", F.row_number().over(w_mesmo_inicio))
        .where(F.col("_rn_inicio") == 1)
        .drop("_rn_inicio", "_dt_evento_regra")
    )

    w_ordem = Window.partitionBy("nr_processo", "cd_sist_origem").orderBy(
        F.col("dt_ini_vig_classe").asc(),
        F.col("cd_seq_tb_origem").asc_nulls_last(),
        F.col("cd_classe").asc(),
    )

    mudancas = (
        base_corrigida
        .withColumn("_cd_classe_ant", F.lag("cd_classe").over(w_ordem))
        .where(F.col("_cd_classe_ant").isNull() | (F.col("cd_classe") != F.col("_cd_classe_ant")))
        .drop("_cd_classe_ant")
    )

    return mudancas.withColumn(
        "dt_fim_vig_classe",
        F.lead("dt_ini_vig_classe").over(w_ordem),
    ).select(
        "nr_processo",
        "cd_sist_origem",
        "cd_seq_tb_origem",
        "cd_classe",
        "dt_ini_vig_classe",
        "dt_fim_vig_classe",
    )


def salvar_full(df: DataFrame, target_table: str, target_exists: bool, num_partitions: int) -> None:
    df_out = df.repartition(num_partitions, "cd_sist_origem")

    if not target_exists:
        logger.info(f"Criando tabela Iceberg {target_table}...")
        (
            df_out.writeTo(target_table)
            .partitionedBy("cd_sist_origem")
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
    affected_keys.createOrReplaceTempView("_proc_classe_vigencia_affected_keys")

    logger.info("Removendo vigencias antigas dos processos afetados...")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING _proc_classe_vigencia_affected_keys k
           ON t.nr_processo = k.nr_processo
          AND t.cd_sist_origem = k.cd_sist_origem
        WHEN MATCHED THEN DELETE
        """
    )

    logger.info("Inserindo vigencias recalculadas dos processos afetados...")
    (
        vigencia_afetados
        .repartition(num_partitions, "cd_sist_origem")
        .writeTo(target_table)
        .append()
    )


def carregar_base(
    spark: SparkSession,
    source_table: str,
    complement_table: str,
    dim_complement_table: str,
    dim_complement_tab_table: str,
    cd_sist_origem: int,
) -> DataFrame:
    return carregar_movimentos_classe(
        spark=spark,
        source_table=source_table,
        complement_table=complement_table,
        dim_complement_table=dim_complement_table,
        dim_complement_tab_table=dim_complement_tab_table,
        cd_sist_origem=cd_sist_origem,
    )


def carregar_distribuicoes(
    spark: SparkSession,
    source_table: str,
    cd_sist_origem: int,
) -> DataFrame:
    return carregar_primeira_distribuicao(spark.table(source_table), cd_sist_origem)


def processar_full(
    spark: SparkSession,
    source_table: str,
    complement_table: str,
    dim_complement_table: str,
    dim_complement_tab_table: str,
    process_class_table: str,
    target_table: str,
    cd_sist_origem: int,
    num_partitions: int,
) -> None:
    movimentos = carregar_base(
        spark,
        source_table,
        complement_table,
        dim_complement_table,
        dim_complement_tab_table,
        cd_sist_origem,
    )
    distribuicoes = carregar_distribuicoes(spark, source_table, cd_sist_origem)
    classe_base = carregar_classe_base_processo(spark, process_class_table, distribuicoes)
    vigencia = gerar_vigencia(movimentos, distribuicoes, classe_base)
    salvar_full(vigencia, target_table, iceberg_table_exists(spark, target_table), num_partitions)


def processar_incremental(
    spark: SparkSession,
    source_table: str,
    complement_table: str,
    dim_complement_table: str,
    dim_complement_tab_table: str,
    process_class_table: str,
    target_table: str,
    cd_sist_origem: int,
    ano: int,
    mes: int,
    num_partitions: int,
) -> None:
    if not iceberg_table_exists(spark, target_table):
        logger.warning(f"{target_table} nao existe. Executando carga full inicial.")
        processar_full(
            spark,
            source_table,
            complement_table,
            dim_complement_table,
            dim_complement_tab_table,
            process_class_table,
            target_table,
            cd_sist_origem,
            num_partitions,
        )
        return

    source_raw = spark.table(source_table)
    movimentos = carregar_base(
        spark,
        source_table,
        complement_table,
        dim_complement_table,
        dim_complement_tab_table,
        cd_sist_origem,
    )
    movimentos_mes = filtrar_movimentos_mes(movimentos, source_raw, ano, mes)

    affected_keys = movimentos_mes.select("nr_processo", "cd_sist_origem").dropDuplicates()
    total_afetados = affected_keys.count()
    logger.info(f"Processos afetados em {ano}{mes:02d}: {total_afetados}")

    if total_afetados == 0:
        logger.info("Nenhuma alteracao de classe nova para atualizar.")
        return

    historico_afetados = movimentos.join(affected_keys, ["nr_processo", "cd_sist_origem"], "inner")
    distribuicoes = carregar_primeira_distribuicao(source_raw, cd_sist_origem)
    distribuicoes_afetados = distribuicoes.join(affected_keys, ["nr_processo", "cd_sist_origem"], "inner")
    classe_base = carregar_classe_base_processo(spark, process_class_table, distribuicoes)
    classe_base_afetados = classe_base.join(affected_keys, ["nr_processo", "cd_sist_origem"], "inner")
    vigencia_afetados = gerar_vigencia(historico_afetados, distribuicoes_afetados, classe_base_afetados)
    substituir_processos_afetados(spark, target_table, affected_keys, vigencia_afetados, num_partitions)


def parse_anomes(anomes: Optional[str]) -> tuple[int, int]:
    if anomes:
        dt = datetime.strptime(anomes, "%Y%m")
    else:
        dt = datetime.now()
    return dt.year, dt.month


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera e atualiza tb_datajud_processo_classe_vigencia a partir dos eventos 14738/14739."
    )
    parser.add_argument("--modo", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--anomes", type=str, help="Mes de referencia YYYYMM para modo incremental.")
    parser.add_argument("--source-table", type=str, default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--complement-table", type=str, default=DEFAULT_COMPLEMENT_TABLE)
    parser.add_argument("--dim-complement-table", type=str, default=DEFAULT_DIM_COMPLEMENT_TABLE)
    parser.add_argument("--dim-complement-tab-table", type=str, default=DEFAULT_DIM_COMPLEMENT_TAB_TABLE)
    parser.add_argument("--process-class-table", type=str, default=DEFAULT_PROCESS_CLASS_TABLE)
    parser.add_argument("--target-table", type=str, default=DEFAULT_TARGET_TABLE)
    parser.add_argument("--cd-sist-origem", type=int, default=1)
    parser.add_argument("--num-partitions", type=int, default=30)
    args = parser.parse_args()

    spark = iniciar_spark()

    try:
        logger.info(f"Source table: {args.source_table}")
        logger.info(f"Target table: {args.target_table}")
        logger.info(f"Modo: {args.modo}")

        if args.modo == "full":
            processar_full(
                spark=spark,
                source_table=args.source_table,
                complement_table=args.complement_table,
                dim_complement_table=args.dim_complement_table,
                dim_complement_tab_table=args.dim_complement_tab_table,
                process_class_table=args.process_class_table,
                target_table=args.target_table,
                cd_sist_origem=args.cd_sist_origem,
                num_partitions=args.num_partitions,
            )
        else:
            ano, mes = parse_anomes(args.anomes)
            processar_incremental(
                spark=spark,
                source_table=args.source_table,
                complement_table=args.complement_table,
                dim_complement_table=args.dim_complement_table,
                dim_complement_tab_table=args.dim_complement_tab_table,
                process_class_table=args.process_class_table,
                target_table=args.target_table,
                cd_sist_origem=args.cd_sist_origem,
                ano=ano,
                mes=mes,
                num_partitions=args.num_partitions,
            )

        logger.info("Processamento concluido.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
