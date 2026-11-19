#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

PATH = "/opt/parquet/data/prata/dim2inst/dim_magistrado_ejud"
KEY_COL = "CODMAG"       # ajuste aqui se a coluna tiver outro nome
KEY_VAL = 1

spark = (
    SparkSession.builder
    .appName("audit_dim_magistrado_ejud_CODMAG1")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("session.tz =>", spark.conf.get("spark.sql.session.timeZone"))
try:
    print("timestampType =>", spark.conf.get("spark.sql.timestampType"))
except Exception:
    print("timestampType => <unset>")

# 1) Ler e filtrar por CODMAG=1
df_all = spark.read.parquet(PATH)
if KEY_COL not in df_all.columns:
    raise RuntimeError(f"Coluna '{KEY_COL}' não encontrada nas colunas: {df_all.columns}")

df = df_all.filter(F.col(KEY_COL) == F.lit(KEY_VAL))
df.printSchema()

# 2) Identificar timestamps e possíveis chaves
ts_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, TimestampType)]
id_candidates = [KEY_COL, "CODMAG", "codpess", "matricula", "sistema", "id", "chave"]
id_cols = [c for c in id_candidates if c in df.columns]

print("id_cols:", id_cols)
print("ts_cols:", ts_cols)

# 3) Amostra (já filtrado)
if ts_cols:
    df.select(*(id_cols + ts_cols)).show(200, False)

# 4) Min/Max (filtrado)
if ts_cols:
    mins = df.select(*[F.min(c).alias(f"{c}__min") for c in ts_cols])
    maxs = df.select(*[F.max(c).alias(f"{c}__max") for c in ts_cols])
    mins.crossJoin(maxs).show(truncate=False)

# 5) Termômetro de hora + nulos (filtrado)
if ts_cols:
    stats = []
    for c in ts_cols:
        stats += [
            F.count(F.when((F.hour(F.col(c))==3) & (F.minute(c)==0) & (F.second(c)==0), 1)).alias(f"{c}__h03"),
            F.count(F.when((F.hour(F.col(c))==0) & (F.minute(c)==0) & (F.second(c)==0), 1)).alias(f"{c}__h00"),
            F.count(F.when(F.col(c).isNull(), 1)).alias(f"{c}__nulls"),
        ]
    df.select(*stats).show(truncate=False)

# 6) Comparar a MESMA leitura em UTC vs America/Sao_Paulo (filtrado por CODMAG=1)
if ts_cols:
    tz0 = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        df_utc = spark.read.parquet(PATH).filter(F.col(KEY_COL) == KEY_VAL).select(*(id_cols + ts_cols))

        spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
        df_sp = spark.read.parquet(PATH).filter(F.col(KEY_COL) == KEY_VAL).select(*(id_cols + ts_cols))

        if id_cols:
            j = df_sp.alias("sp").join(df_utc.alias("utc"), on=id_cols, how="inner")
        else:
            # fallback sem chave: join por ordem da amostra filtrada
            w = Window.orderBy(F.monotonically_increasing_id())
            j = (df_sp.withColumn("_rn", F.row_number().over(w))
                   .join(df_utc.withColumn("_rn", F.row_number().over(w)), "_rn")
                   .drop("_rn"))

        diffs = [
            F.round((F.col(f"sp.`{c}`").cast("long") - F.col(f"utc.`{c}`").cast("long"))/3600.0, 2).alias(f"{c}__diff_h")
            for c in ts_cols
        ]
        j.select(*(id_cols + diffs)).show(200, False)
    finally:
        spark.conf.set("spark.sql.session.timeZone", tz0)

spark.stop()

