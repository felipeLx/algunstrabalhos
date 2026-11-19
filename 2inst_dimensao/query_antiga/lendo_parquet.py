from pyspark.sql import SparkSession

# Inicializa a sessão do Spark
spark = SparkSession.builder \
    .appName("LeitorParquet") \
    .getOrCreate()

# Caminho para o seu arquivo Parquet
file_path = "/opt/parquet/data/prata/dim2inst/dim_magistrado_ejud/dim_magistrado_ejud.parquet"

try:
    # Lê o arquivo Parquet
    df = spark.read.parquet(file_path)

    # Exibe o esquema do DataFrame para verificar os tipos de dados
    print("=== Esquema do DataFrame ===")
    df.printSchema()

    # Exibe uma amostra dos dados para verificar o conteúdo
    print("\n=== Amostra dos Dados ===")
    #df.select("id_pessoa", "nome_magistrado").show(truncate=False)
    df.show()

except Exception as e:
    print(f"Ocorreu um erro ao ler o arquivo Parquet: {e}")

finally:
    # Para a sessão do Spark
    spark.stop()
