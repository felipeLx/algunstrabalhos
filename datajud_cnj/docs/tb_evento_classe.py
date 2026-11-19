# -*- coding: utf-8 -*-
import logging
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F


# =========================
# Constantes
# =========================
APP_NAME = "etl_dcp_tb_evento_classe"

DEFAULT_OUTPUT_TABLE = "prata.tb_dcp_evento_classe"
DEFAULT_NUM_PARTITIONS = 60


# =========================
# Logging
# =========================
def setup_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(APP_NAME)


logger = setup_logger()


# =========================
# SparkSession
# =========================
def iniciar_spark(app_name: str = APP_NAME) -> SparkSession:
    logger.info("Inicializando SparkSession...")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.executor.instances", "20")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "7g")
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.cores", "2")
        .config("spark.sql.shuffle.partitions", "80")
        .config("spark.memory.fraction", "0.8")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Iceberg extension (catálogo já configurado no cluster)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession pronta.")
    return spark


# =========================
# Transformação principal
# =========================
def cria_tb_evento_classe(spark: SparkSession) -> DataFrame:
    """
    Associa cd_classe ao processo_evento usando histórico de classe e fallback no processo.
    Obs: Mantém colunas ano/mes apenas como conveniência analítica (não particiona a tabela).
    """
    logger.info("Associando classes aos movimentos (processo_evento)...")

    query = """
        WITH historico AS (
            SELECT *
              FROM prata.tb_dcp_processo_hist_classe
        ),
        movimentos_classe AS (
            SELECT
                tpe.id_processo,
                tpe.id_processo_evento,
                tpe.dth_evento,
                COALESCE(
                    h14739_pre.cd_classe_anterior,
                    CASE
                        WHEN h14739_post.rn IS NULL THEN h14738.cd_classe_atual
                        ELSE NULL
                    END,
                    h_antes.cd_classe_atual,
                    pc.cod_classe
                ) AS cd_classe
            FROM prata.tb_dcp_processo_evento tpe

            LEFT JOIN (
                SELECT
                    id_processo,
                    cd_classe_atual,
                    dth_evento,
                    ROW_NUMBER() OVER (
                        PARTITION BY id_processo
                        ORDER BY dth_evento DESC
                    ) AS rn
                  FROM historico
                 WHERE cd_evento = 14738
            ) h14738
              ON tpe.id_processo = h14738.id_processo
             AND tpe.dth_evento < h14738.dth_evento
             AND h14738.rn = 1

            LEFT JOIN (
                SELECT
                    id_processo,
                    cd_classe_anterior,
                    dth_evento,
                    ROW_NUMBER() OVER (
                        PARTITION BY id_processo
                        ORDER BY dth_evento
                    ) AS rn
                  FROM historico
                 WHERE cd_evento = 14739
            ) h14739_pre
              ON tpe.id_processo = h14739_pre.id_processo
             AND tpe.dth_evento < h14739_pre.dth_evento
             AND h14739_pre.rn = 1

            LEFT JOIN (
                SELECT
                    id_processo,
                    cd_classe_atual,
                    dth_evento,
                    ROW_NUMBER() OVER (
                        PARTITION BY id_processo
                        ORDER BY dth_evento
                    ) AS rn
                  FROM historico
                 WHERE cd_evento = 14739
            ) h14739_post
              ON tpe.id_processo = h14739_post.id_processo
             AND tpe.dth_evento < h14739_post.dth_evento
             AND h14739_post.rn = 1

            LEFT JOIN (
                SELECT
                    id_processo,
                    cd_classe_atual,
                    dth_evento,
                    ROW_NUMBER() OVER (
                        PARTITION BY id_processo
                        ORDER BY dth_evento DESC
                    ) AS rn
                  FROM historico
                 WHERE dth_evento <= CURRENT_DATE
            ) h_antes
              ON tpe.id_processo = h_antes.id_processo
             AND tpe.dth_evento >= h_antes.dth_evento
             AND h_antes.rn = 1

            LEFT JOIN dcp.processo pc
              ON tpe.id_processo = pc.cod_proc
        )
        SELECT DISTINCT
               mc.id_processo,
               mc.id_processo_evento,
               mc.dth_evento,
               CAST(DATE_FORMAT(mc.dth_evento, 'yyyy') AS INT) AS ano,
               CAST(DATE_FORMAT(mc.dth_evento, 'MM')   AS INT) AS mes,
               mc.cd_classe
          FROM movimentos_classe mc
    """
    
    return spark.sql(query)


# =========================
# Iceberg helpers (SEM particionamento)
# =========================
def iceberg_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    try:
        spark.table(full_table_name).limit(1).collect()
        return True
    except Exception:
        return False


def gravar_em_iceberg(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
    num_partitions: int = DEFAULT_NUM_PARTITIONS,
    write_mode: str = "overwrite",
) -> None:
    """
    Grava df na tabela Iceberg (schema.tabela) SEM partition spec.

    - Se não existir: create()
    - Se existir:
        - overwrite: sobrescreve a tabela inteira (padrão recomendado aqui)
        - append: append()
    """
    logger.info("⛏️ Preparando DF para escrita Iceberg…")

    if not df.head(1):
        logger.info("DataFrame vazio. Nada a gravar.")
        return

    df_write = df

    # Controle de arquivos: particiona por chave natural (não por ano/mes)
    if num_partitions and num_partitions > 0:
        df_write = df_write.repartition(num_partitions, col("id_processo"))

    exists = iceberg_table_exists(spark, full_table_name)

    if not exists:
        logger.info(f"🧊 Tabela Iceberg não existe. Criando: {full_table_name}")
        (
            df_write.writeTo(full_table_name)
            .tableProperty("format-version", "2")
            .create()
        )
        logger.info("✅ Tabela criada e dados gravados.")
        return

    if write_mode == "append":
        logger.info(f"🧊 Tabela existe. APPEND em {full_table_name}")
        df_write.writeTo(full_table_name).append()
        logger.info("✅ Append concluído.")
        return

    if write_mode == "overwrite":
        logger.info(f"🧊 Tabela existe. OVERWRITE TOTAL em {full_table_name}")
        df_write.writeTo(full_table_name).overwrite(F.expr("true"))
        logger.info("✅ Overwrite concluído.")
        return

    raise ValueError("write_mode inválido. Use 'overwrite' ou 'append'.")


# =========================
# main
# =========================
def main():
    parser = argparse.ArgumentParser(description="ETL tb_evento_classe -> Iceberg (prata)")
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_TABLE,
        help="Tabela Iceberg destino (ex: prata.tb_dcp_evento_classe)",
    )
    parser.add_argument(
        "--write-mode",
        type=str,
        default="overwrite",
        choices=["overwrite", "append"],
        help="Modo: overwrite (reprocessamento total) ou append (incremental).",
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=DEFAULT_NUM_PARTITIONS,
        help="Repartition antes de gravar (controle de arquivos).",
    )

    args = parser.parse_args()
    dest_table = args.output.strip()

    # Proteção: evita passar path S3 por engano
    if dest_table.startswith("s3://") or dest_table.startswith("s3a://"):
        raise ValueError(
            f"--output deve ser uma TABELA Iceberg (ex: prata.tb_dcp_evento_classe). "
            f"Você passou um path: {dest_table}"
        )

    spark = None

    try:
        logger.info("Iniciando o script tb_evento_classe...")
        spark = iniciar_spark()

        df = cria_tb_evento_classe(spark)

        logger.info(f"🧊 Gravando em Iceberg: {dest_table} (mode={args.write_mode})…")
        gravar_em_iceberg(
            spark=spark,
            df=df,
            full_table_name=dest_table,
            num_partitions=args.num_partitions,
            write_mode=args.write_mode,
        )

        spark.catalog.clearCache()
        logger.info("✅ Processamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"❌ Ocorreu um erro durante a execução: {e}", exc_info=True)
        raise

    finally:
        if spark is not None:
            spark.catalog.clearCache()
            spark.stop()
            logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
