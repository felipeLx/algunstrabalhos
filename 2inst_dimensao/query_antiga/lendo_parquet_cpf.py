# -*- coding: utf-8 -*-
# Auditoria de CPF em Parquets (somente leitura)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, length, regexp_replace, count, countDistinct, sum as _sum, expr, lit
)

spark = (SparkSession.builder
         .appName("AuditCPFMagistrados")
         .getOrCreate())

# ---- Paths informados ----
PATHS = [
    ("/opt/parquet/data/prata/dim2inst/dim_magistrado_ejud",  "dim_magistrado_ejud",  ["codmag", "nome"],                    "cpf"),
    ("/opt/parquet/data/prata/dim2inst/dim_magistrado_eproc", "dim_magistrado_eproc", ["cod_magistrado", "nome_magistrado"], "cpf"),
    ("/opt/parquet/data/ouro/dim2inst/dim_magistrado",        "dim_magistrado",       ["codmag_ejud", "nome_ejud"],         "cpf"),
]

def audit_parquet(path, label, id_cols, cpf_col):
    print("\n" + "="*88)
    print(f"===> AUDITANDO: {label} | Parquet: {path}")
    print("="*88)

    df = spark.read.parquet(path)

    # 1) Schema e tipo do CPF
    print("\n-- Schema --")
    df.printSchema()

    cols_lower = {c.lower(): c for c in df.columns}
    if cpf_col.lower() not in cols_lower:
        # tenta localizar alguma coluna parecida
        possiveis = [c for c in df.columns if "cpf" in c.lower()]
        print(f"\n[AVISO] Coluna '{cpf_col}' não encontrada. Possíveis: {possiveis}")
        return

    cpf_real_col = cols_lower[cpf_col.lower()]
    cpf_dtype = dict(df.dtypes).get(cpf_real_col)
    print(f"\nColuna CPF encontrada: {cpf_real_col} (dtype: {cpf_dtype})")

    # 2) Amostra "crua" (como está gravado)
    print("\n-- Amostra crua (cpf como está no arquivo) --")
    sample_cols = [c for c in id_cols if c in df.columns] + [cpf_real_col]
    df.select(*sample_cols).show(20, truncate=False)

    # 3) Métricas de qualidade
    df_a = (
        df
        .select(
            col(cpf_real_col).alias("cpf_raw")
        )
        .withColumn("cpf_str", col("cpf_raw").cast("string"))
        .withColumn("cpf_digits", regexp_replace(col("cpf_str"), r"\D", ""))  # só dígitos
        .withColumn("len_raw",    length(col("cpf_str")))
        .withColumn("len_digits", length(col("cpf_digits")))
        .withColumn("has_non_digits", (col("len_raw") != col("len_digits")))  # tem pontuação/letra/etc
        .withColumn(
            "possible_zero_loss",
            # heurística: se só tem dígitos e len_digits < 11, pode ter perdido zero à esquerda
            (expr("cpf_str rlike '^[0-9]+$'") & (col("len_digits") < lit(11)))
        )
    )

    print("\n-- Métricas gerais --")
    df_a.agg(
        count("*").alias("linhas"),
        _sum(col("cpf_raw").isNull().cast("int")).alias("nulls"),
        countDistinct("cpf_raw").alias("distinct_raw"),
        countDistinct("cpf_digits").alias("distinct_digits"),
        _sum(col("has_non_digits").cast("int")).alias("com_pontuacao_ou_letras"),
        _sum((col("len_digits") == 11).cast("int")).alias("len_digits_11"),
        _sum((col("len_digits") < 11).cast("int")).alias("len_digits_<11"),
        _sum((col("len_digits") > 11).cast("int")).alias("len_digits_>11"),
        _sum(col("possible_zero_loss").cast("int")).alias("possivel_perda_zero_esquerda")
    ).show(truncate=False)

    print("\n-- Distribuição por len_digits (após remover não dígitos) --")
    (df_a.groupBy("len_digits")
         .count()
         .orderBy("len_digits")
         .show(50, truncate=False))

    print("\n-- Exemplos 'problemáticos' (len_digits != 11) --")
    (df_a.filter(col("len_digits") != 11)
         .select("cpf_raw", "cpf_str", "cpf_digits", "len_raw", "len_digits", "has_non_digits", "possible_zero_loss")
         .distinct()
         .show(20, truncate=False))

    print("\n-- Exemplos 'ok' (primeiros 10 com 11 dígitos) --")
    (df_a.filter(col("len_digits") == 11)
         .select("cpf_raw", "cpf_str")
         .distinct()
         .show(10, truncate=False))

# Rodar auditoria para cada conjunto
for path, label, id_cols, cpf_col in PATHS:
    audit_parquet(path, label, id_cols, cpf_col)

