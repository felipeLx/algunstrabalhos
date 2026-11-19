#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("seed_dw2inst_magistrados_cpf_sanitizado")
        .getOrCreate()
    )

    # 1) Create database (Glue)
    spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.dw2inst")

    # 2) Create Iceberg table in PRATA
    spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.dw2inst.magistrados_cpf_sanitizado (
      cpf       STRING,
      nome      STRING,
      nome_ejud STRING
    )
    USING iceberg
    LOCATION 's3://sgdai-data-lake/prata/dw2inst/magistrados_cpf_sanitizado'
    TBLPROPERTIES (
      'format-version'='2',
      'identifier-fields'='cpf',
      'write.parquet.compression-codec'='zstd'
    )
    """)

    # 3) Insert overwrite (seed)
    spark.sql("""
    INSERT OVERWRITE glue_catalog.dw2inst.magistrados_cpf_sanitizado
    VALUES
    ('01591045720','MOACYR MARQUES MORADO','DES. MOACYR MARQUES MORADO'),
    ('02946599787','NELSON MARTINS FERREIRA','DES. NELSON MARTINS FERREIRA'),
    ('03473325791','JULIO ALBERTO ALVARES','DES. JULIO ALBERTO ALVARES'),
    ('01513597922','MARCELO COSTA','DES. MARCELO COSTA'),
    ('18281850787','RUTE VIANA LINS','JDS. DES. RUTE VIANA LINS'),
    ('35113596787','MARUF ARIDE','JDS. DES. MARUF ARIDE'),
    ('30990394700','HELEN NAVEGA','JDS. DES. HELEN NAVEGA'),
    ('09022633748','OLIVEIRA RAMOS','DES. OLIVEIRA RAMOS'),
    ('02865211720','MARIO GUARACI DE CARVALHO RANGEL','DES. MARIO GUARACI DE CARVALHO RANGEL'),
    ('02898018775','RONALD DE SOUZA','DES. RONALD DE SOUZA'),
    ('00301884749','ALBERTO LACERDA FILHO','DES. ALBERTO LACERDA'),
    ('00811610772','LUIS ANTONIO DE ANDRADE','DES. LUIS ANTONIO DE ANDRADE'),
    ('00002057107','OSWALDO GOULART PIRES','DES. GOULART PIRES'),
    ('00384593704','GRACHO AURELIO SA VIANNA PEREIRA DE VASCONCELLOS','DES. GRACCHO AURELIO'),
    ('43318576700','SALVADOR PINTO FILHO','DES. SALVADOR PINTO')
    """)

    # 4) Quick validation logs
    df = spark.sql("SELECT count(*) AS qt FROM glue_catalog.dw2inst.magistrados_cpf_sanitizado")
    qt = df.collect()[0]["qt"]
    print(f"[OK] Seed concluído. Total registros: {qt}")

    spark.stop()

if __name__ == "__main__":
    main()

