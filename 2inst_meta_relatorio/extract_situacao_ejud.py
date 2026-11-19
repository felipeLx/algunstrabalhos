#!/usr/bin/env python3
"""Extract EJUD situacao (first/last distrib) and save parquet (and optional Postgres).

Usage:
  python scripts/extract_situacao_ejud.py --start 2024-01-01 --end 2024-12-31 --out /opt/parquet/data/ouro/dim2inst/dim_codoj_map_ejud_situacao

This script is standalone: it creates a DataFrame limited to the provided date range,
aggregates first/last situacao events (codes 24,61), joins OJ->magistrate mapping,
and writes a parquet folder suitable for reuse by the meta pipeline.
"""
import argparse
import os
from typing_extensions import final
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, struct, min as spark_min, max as spark_max
from pathlib import Path

from utils.parquet_utils import configure_spark_for_storage, ejud_silver, bronze_movimentorelator, ejud_bronze_orgao, parquet_path, ensure_local_directory
from utils.save_to_postgres import save_df_to_postgres_no_jar


def build_spark(app_name="extract_situacao_ejud"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    configure_spark_for_storage(spark)
    return spark


def extract():
    spark = build_spark()

    situacao_path = "/opt/parquet/data/bronze/ejud/ultimadistribuicaoriger/ultimadistribuicaoriger.parquet"
    print(f"Reading situacao from: {situacao_path}")

    # Lendo apenas a coluna de data para economizar memória
    # O Spark lerá as colunas de partição (year_month) e a dthrmov
    situacao_raw = spark.read.parquet(situacao_path).select("dthrmov")

    # 1. Investigação de Datas (Min e Max)
    # Usamos agg() para que o Spark faça o cálculo distribuído nos executors
    stats = situacao_raw.agg(
        spark_min("dthrmov").alias("data_minima"),
        spark_max("dthrmov").alias("data_maxima")
    ).collect()[0]

    print("-" * 30)
    print(f"RESULTADO DA INVESTIGAÇÃO:")
    print(f"Menor data encontrada (dthrmov): {stats['data_minima']}")
    print(f"Maior data encontrada (dthrmov): {stats['data_maxima']}")
    print("-" * 30)

    # 2. Verificação de Schema e Amostra
    situacao_raw.printSchema()
    # Mostra as 5 linhas mais recentes usando uma lógica eficiente
    print("Amostra das 5 linhas mais recentes:")
    situacao_raw.orderBy(col("dthrmov").desc()).show(5, truncate=False)

    print("Done.")

def main():

    extract()


if __name__ == "__main__":
    main()
