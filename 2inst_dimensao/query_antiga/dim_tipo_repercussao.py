from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

# ── 1. Leitura das dimensões prata ──────────────────────────────────────────
PATH_EJUD  = "/opt/parquet/data/prata/dim2inst/dim_tipo_repercussao_ejud"
PATH_EPROC = "/opt/parquet/data/prata/dim2inst/dim_tipo_repercussao_eproc"
PATH_OURO  = "/opt/parquet/data/ouro/dim2inst/dim_tipo_repercussao"

df_ejud  = spark.read.parquet(PATH_EJUD)
df_eproc = spark.read.parquet(PATH_EPROC)

# ── 2. Bridge: (cod_ejud → cod_eproc) com descrição canônica ────────────────
# Descrição canônica = usamos o EJUD como fonte da verdade
bridge_data = [
    (2,  "Tema",   9),
    (2,  "Tema",   13),
    (3,  "IRDR",   19),
    (3,  "IRDR",   27),
    (4,  "IAC",    14),
    (4,  "IAC",    17),
    (4,  "IAC",    22),
    (4,  "IAC",    26),
    (5,  "GR",     28),
    (6,  "SIRDR",  15),
    (6,  "SIRDR",  16),
    (7,  "ADI",    30),
    (7,  "ADI",    68),
    (8,  "ADC",    31),
    (8,  "ADC",    69),
    (9,  "ADO",    33),
    (10, "ADPF",   32),
    (11, "PUIL",   29),
]

bridge_schema = StructType([
    StructField("cod_unificado",   IntegerType()),
    StructField("desc_unificada",  StringType()),
    StructField("cod_eproc_orig",  IntegerType()),
])

df_bridge = spark.createDataFrame(bridge_data, bridge_schema)

# ── 3. Montar registros EJUD na dimensão ouro ────────────────────────────────
# Todos os códigos EJUD entram diretamente (cod_original = cod_unificado)
df_ejud_ouro = (
    df_ejud
    .select(
        F.col("cod_tipo_repercussao").alias("cod_unificado"),
        F.col("desc_tipo_repercussao").alias("desc_unificada"),
        F.lit("EJUD").alias("sistema"),
        F.col("cod_tipo_repercussao").alias("cod_original"),
    )
)

# ── 4. Montar registros EPROC na dimensão ouro ───────────────────────────────
# Cada cod_eproc recebe o cod_unificado e desc_unificada via bridge
df_eproc_ouro = (
    df_eproc
    .join(
        df_bridge,
        df_eproc["cod_tipo_repercussao"] == df_bridge["cod_eproc_orig"],
        how="inner"  # EPROCs sem mapeamento ficam de fora — ajuste para "left" se necessário
    )
    .select(
        F.col("cod_unificado"),
        F.col("desc_unificada"),
        F.lit("EPROC").alias("sistema"),
        df_eproc["cod_tipo_repercussao"].alias("cod_original"),
    )
)

# ── 5. União e gravação na camada ouro ───────────────────────────────────────
df_ouro = df_ejud_ouro.unionByName(df_eproc_ouro).orderBy("cod_unificado", "sistema")

df_ouro.write \
    .mode("overwrite") \
    .parquet(PATH_OURO)
if __name__ == "__main__":
    df_ouro.write.mode("overwrite").parquet(PATH_OURO)
    print(f"Dimensão ouro gravada em: {PATH_OURO}")
    df_ouro.show(50, truncate=False)