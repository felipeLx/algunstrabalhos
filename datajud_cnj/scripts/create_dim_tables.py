#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Tuple, Optional

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# =============================================================================
# Defaults / Identidade
# =============================================================================
DB_NAME = "dim1inst"
TABLE_NAME = "dim_tb_sist_orig"

DEFAULT_S3_BASE = "s3://sgdai-data-lake/ouro/dim1inst/"
DEFAULT_TABLE_LOCATION = f"{DEFAULT_S3_BASE}{TABLE_NAME}/"


# =============================================================================
# Logger
# =============================================================================
def get_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("create_dim_tables")


logger = get_logger()


# =============================================================================
# Args / Spark
# =============================================================================
def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cria/atualiza dim1inst.dim_tb_sist_orig no Glue Data Catalog (Athena) a partir de Parquet no S3."
    )
    p.add_argument(
        "--output-path",
        default=DEFAULT_TABLE_LOCATION,
        help=f"S3 path dos Parquets. Default: {DEFAULT_TABLE_LOCATION}",
    )
    p.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Modo de escrita no S3. Default: overwrite.",
    )
    p.add_argument(
        "--compression",
        default="snappy",
        help="Compressão Parquet. Default: snappy.",
    )
    p.add_argument(
        "--coalesce",
        type=int,
        default=1,
        help="Número de arquivos de saída. Default: 1 (dimensão pequena).",
    )
    p.add_argument(
        "--glue-region",
        default=None,
        help="Região AWS do Glue (opcional). Se omitido, usa região padrão do ambiente/SDK.",
    )
    p.add_argument(
        "--glue-table-name",
        default=TABLE_NAME,
        help=f"Nome da tabela no Glue. Default: {TABLE_NAME}",
    )
    p.add_argument(
        "--glue-db-name",
        default=DB_NAME,
        help=f"Nome do database no Glue. Default: {DB_NAME}",
    )
    return p.parse_args(argv)


def get_spark(app_name: str = "create_dim1inst_dim_tb_sist_orig") -> SparkSession:
    logger.info("Criando SparkSession (app=%s)", app_name)
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# =============================================================================
# Fonte (equivalente ao UNION ALL)
# =============================================================================
def source_rows_dim_tb_sist_orig() -> List[Tuple[int, int, str, str]]:
    return [
        (1, 2, "CORE", "TB_PROCESSO_EVENTO"),
        (2, 3, "EPROC1G", "PROCESSO_EVENTO"),
        (3, 1, "DCP", "ANDAMENTOPROCESSO"),
        (4, 1, "DCP", "AUDIENCIA"),
        (5, 1, "DCP", "DISTRIBUICAOPROCESSO"),
        (6, 1, "DCP", "SITUACAOPROCESSO"),
        (7, 1, "DCP", "PROCESSOCLASSESALTERADAS"),
        (8, 1, "PROCCORP", "INTIMACAOELETRONICAORGAO"),
        (9, 1, "DW_1INST", "MVM_MOV_SCJ"),
        (10, 1, "DW_1INST", "DATAJUD_PRECATORIOSSUSPCANC"),
    ]


def build_df_dim_tb_sist_orig(spark: SparkSession) -> DataFrame:
    logger.info("Montando DataFrame da dimensão %s.%s", DB_NAME, TABLE_NAME)
    schema = StructType(
        [
            StructField("cd_tabela", IntegerType(), False),
            StructField("cd_sist_orig", IntegerType(), False),
            StructField("ds_schema_orig", StringType(), False),
            StructField("ds_tb_orig", StringType(), False),
        ]
    )
    df = spark.createDataFrame(source_rows_dim_tb_sist_orig(), schema=schema)
    logger.info("DataFrame criado com %d registros", df.count())
    return df


# =============================================================================
# Escrita no S3
# =============================================================================
def write_parquet(df: DataFrame, output_path: str, mode: str, compression: str, coalesce_n: int) -> None:
    path = output_path.rstrip("/") + "/"
    logger.info("Gravando Parquet no S3")
    logger.info("Path=%s | Mode=%s | Compression=%s | Coalesce=%s", path, mode, compression, coalesce_n)

    writer_df = df.coalesce(coalesce_n) if coalesce_n and coalesce_n > 0 else df

    (
        writer_df.write.mode(mode)
        .format("parquet")
        .option("compression", compression)
        .save(path)
    )

    logger.info("Gravação no S3 concluída: %s", path)


def validate_s3_readback(spark: SparkSession, output_path: str) -> int:
    """Validação simples: tenta ler o parquet gravado e retorna count."""
    path = output_path.rstrip("/") + "/"
    logger.info("Validando leitura do Parquet via Spark: %s", path)
    cnt = spark.read.parquet(path).count()
    logger.info("Readback OK: %d registros lidos do Parquet", cnt)
    return cnt


# =============================================================================
# Glue (boto3): cria/atualiza database e table
# =============================================================================
def get_glue_client(region: Optional[str] = None):
    logger.info("Inicializando cliente Glue (region=%s)", region or "default")
    return boto3.client("glue", region_name=region) if region else boto3.client("glue")


def ensure_glue_database(glue, db_name: str) -> None:
    """Garante que o database existe no Glue."""
    logger.info("Garantindo database no Glue: %s", db_name)
    try:
        glue.get_database(Name=db_name)
        logger.info("Database já existe: %s", db_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": db_name})
        logger.info("Database criado: %s", db_name)


def glue_table_input_parquet_external(
    table_name: str,
    s3_location: str,
) -> dict:
    """
    Monta TableInput "limpo" (compatível com Athena) para Parquet EXTERNAL.
    Evita properties do Spark/Iceberg que fazem o Athena reclamar.
    """
    loc = s3_location.rstrip("/") + "/"

    columns = [
        {"Name": "cd_tabela", "Type": "int"},
        {"Name": "cd_sist_orig", "Type": "int"},
        {"Name": "ds_schema_orig", "Type": "string"},
        {"Name": "ds_tb_orig", "Type": "string"},
    ]

    # SerDe/InputFormat/OutputFormat padrão do Parquet no Athena/Glue
    storage_descriptor = {
        "Columns": columns,
        "Location": loc,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {
                "serialization.format": "1"
            },
        },
        # Sem partições, sem buckets
        "Compressed": False,
        "NumberOfBuckets": -1,
        "StoredAsSubDirectories": False,
        "Parameters": {},
    }

    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE",
            "classification": "parquet",
        },
        "StorageDescriptor": storage_descriptor,
        "PartitionKeys": [],
    }

    return table_input


def create_or_update_glue_table(glue, db_name: str, table_name: str, s3_location: str) -> None:
    logger.info("Criando/atualizando tabela no Glue: %s.%s", db_name, table_name)
    table_input = glue_table_input_parquet_external(table_name=table_name, s3_location=s3_location)

    try:
        glue.get_table(DatabaseName=db_name, Name=table_name)
        # Existe -> update
        glue.update_table(DatabaseName=db_name, TableInput=table_input)
        logger.info("Tabela atualizada no Glue: %s.%s", db_name, table_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=db_name, TableInput=table_input)
        logger.info("Tabela criada no Glue: %s.%s", db_name, table_name)


# =============================================================================
# Runner
# =============================================================================
def run_dim_tb_sist_orig(
    spark: SparkSession,
    glue,
    output_path: str,
    mode: str,
    compression: str,
    coalesce_n: int,
    glue_db_name: str,
    glue_table_name: str,
) -> None:
    logger.info("---- INÍCIO: %s.%s ----", glue_db_name, glue_table_name)

    # 1) DF
    df = build_df_dim_tb_sist_orig(spark)

    # 2) S3 Parquet
    write_parquet(df, output_path=output_path, mode=mode, compression=compression, coalesce_n=coalesce_n)

    # 3) Readback (garante que o path e arquivos parquet estão OK)
    cnt = validate_s3_readback(spark, output_path=output_path)
    if cnt == 0:
        raise RuntimeError(
            f"Readback retornou 0 registros. Verifique se o path está correto: {output_path.rstrip('/')}/"
        )

    # 4) Glue database/table (via boto3)
    ensure_glue_database(glue, glue_db_name)
    create_or_update_glue_table(glue, glue_db_name, glue_table_name, s3_location=output_path)

    logger.info("---- FIM: %s.%s (OK) ----", glue_db_name, glue_table_name)


def main(argv: List[str]) -> int:
    logger.info("==== INÍCIO DO JOB create_dim_tables ====")
    args = parse_args(argv)

    logger.info(
        "Parâmetros: output_path=%s | mode=%s | compression=%s | coalesce=%s | glue_db=%s | glue_table=%s | region=%s",
        args.output_path, args.mode, args.compression, args.coalesce, args.glue_db_name, args.glue_table_name, args.glue_region
    )

    spark = get_spark()
    glue = get_glue_client(region=args.glue_region)

    try:
        run_dim_tb_sist_orig(
            spark=spark,
            glue=glue,
            output_path=args.output_path,
            mode=args.mode,
            compression=args.compression,
            coalesce_n=args.coalesce,
            glue_db_name=args.glue_db_name,
            glue_table_name=args.glue_table_name,
        )

        logger.info("JOB finalizado com sucesso.")
        logger.info("Athena (Catálogo): AwsDataCatalog.%s.%s", args.glue_db_name, args.glue_table_name)
        logger.info("S3: %s", args.output_path.rstrip("/") + "/")
        return 0

    except Exception:
        logger.exception("Erro fatal durante a execução do job.")
        return 1

    finally:
        try:
            spark.stop()
            logger.info("SparkSession encerrada.")
        except Exception:
            logger.warning("Falha ao encerrar SparkSession (ignorado).")
        logger.info("==== FIM DO JOB create_dim_tables ====")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
