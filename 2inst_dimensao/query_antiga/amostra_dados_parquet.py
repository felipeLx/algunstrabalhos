from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Defina o caminho para o arquivo parquet gerado
parquet_path = "/opt/parquet/data/prata/dim2inst/dim_magistrado_eproc"

try:
    # Lê o arquivo Parquet
    df = spark.read.parquet(parquet_path)

    print("=== Esquema do DataFrame ===")
    df.printSchema()

    print("\n=== Amostra dos Dados ===")
    df.select("id_pessoa", "nome_magistrado").show(20, truncate=False)

    df.limit(5).show(truncate=False)

except Exception as e:
    print(f"Erro ao ler o arquivo Parquet: {e}")
