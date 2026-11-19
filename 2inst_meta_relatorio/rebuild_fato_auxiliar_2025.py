#!/usr/bin/env python3
"""
rebuild_fato_auxiliar_2025.py
===========================================
Reconstrói a fato_auxiliar para o ano 2025 a partir dos parquets da camada prata.

Fonte principal: /opt/parquet/data/prata/ejud/meta_cnj2025/meta_*/current/*.parquet
                 (meta_01 a meta_11 — todas as disponíveis são auto-descobertas)

Passos:
  1. Descobre todos os meta_NN/current nos subdiretórios de prata/ejud/meta_cnj2025
  2. Coleta todos os id_processo distintos de todas as variáveis
  3. Enriquece com datas de distribuição via bronze/ejud/t_processo
  4. Obtém último evento via prata/ejud/tab_xml_andamento + bronze/ejud/movimentorelator
  5. Obtém ind_baixado (baixm*.parquet) e ind_julgado (julgadom*.parquet)
  6. Obtém id_assunto_principal via bronze/ejud/processoassunto
  7. Deduplica por id_processo (mantém linha com evento mais recente)
  8. Grava em dw2inst.temp_fatoauxiliar via COPY (bulk, rápido)
  9. Reescreve parquet limpo em ouro/fato_auxiliar_2025_clean

Uso no servidor:
  cd /opt/parquet/projetos/2inst_meta_relatorio
  python rebuild_fato_auxiliar_2025.py [--skip-postgres] [--skip-parquet] [--dry-run]

Flags opcionais:
  --skip-postgres   Pula a gravação no PostgreSQL
  --skip-parquet    Pula a reescrita do parquet limpo
  --dry-run         Apenas mostra contagens, não grava nada
"""

import os
import sys
import logging
import argparse
import time
import io
import glob
from datetime import datetime

from dotenv import load_dotenv

PARQUET_HOME = "/opt/parquet"
load_dotenv(dotenv_path=os.path.join(PARQUET_HOME, "airflow", "dotenv", ".env"), override=True)
sys.path.insert(0, os.path.join(PARQUET_HOME, "projetos"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"/tmp/rebuild_fato_auxiliar_2025_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
logger = logging.getLogger("rebuild_fato_auxiliar_2025")

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------
PRATA_EJUD_2025    = os.path.join(PARQUET_HOME, "data", "prata", "ejud", "meta_cnj2025")
BRONZE_T_PROCESSO  = os.path.join(PARQUET_HOME, "data", "bronze", "ejud", "t_processo")
BRONZE_MOVRELATOR  = os.path.join(PARQUET_HOME, "data", "bronze", "ejud", "movimentorelator")
PRATA_TAB_XML      = os.path.join(PARQUET_HOME, "data", "prata", "ejud", "tab_xml_andamento")
FATO_CLEAN_PATH    = os.path.join(PARQUET_HOME, "data", "ouro", "fato_auxiliar_2025_clean")
META_ANO_TARGET    = 2025

# Lista de metas (fallback — a descoberta automática tem prioridade)
ALL_METAS = ["01", "02", "04", "06", "08", "10", "11"]

# Colunas esperadas (ordem importa para o COPY)
FATO_COLUMNS = [
    "id_processo",
    "dth_prim_distribuicao",
    "dth_ult_distribuicao",
    "cod_prim_oj",
    "cod_ult_oj",
    "cod_prim_magistrado",
    "cod_ult_magistrado",
    "dth_ult_evento",
    "cod_ult_evento_cnj",
    "ind_baixado",
    "ind_julgado",
    "id_sigilo",
    "complemento",
    "meta_ano",
]

# DDL da tabela temporária
CREATE_TEMP_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dw2inst.temp_fatoauxiliar (
    id_processo           varchar(50),
    dth_prim_distribuicao timestamp,
    dth_ult_distribuicao  timestamp,
    cod_prim_oj           integer,
    cod_ult_oj            integer,
    cod_prim_magistrado   integer,
    cod_ult_magistrado    integer,
    dth_ult_evento        timestamp,
    cod_ult_evento_cnj    integer,
    ind_baixado           smallint,
    ind_julgado           smallint,
    id_sigilo             smallint,
    complemento           text,
    meta_ano              integer
);
"""

TRUNCATE_TEMP_SQL = "TRUNCATE TABLE dw2inst.temp_fatoauxiliar;"

# ---------------------------------------------------------------------------
# Helpers de conexão
# ---------------------------------------------------------------------------

def _get_pg_connection():
    import psycopg2
    pg_host = os.environ.get("POSTGRESQL_SL_HOST", os.environ.get("PG_HOST", "172.20.30.141"))
    pg_port = int(os.environ.get("POSTGRESQL_SL_PORT", os.environ.get("PG_PORT", "5432")))
    pg_db   = os.environ.get("POSTGRESQL_SL_DBNAME", os.environ.get("PG_DB", "postgres"))
    pg_user = os.environ.get("POSTGRESQL_SL_USER", os.environ.get("PG_USER", "felipe.lisboa"))
    pg_pass = os.environ.get("POSTGRESQL_SL_PASSWORD", os.environ.get("PG_PASS", ""))
    return psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pass)


def _ensure_temp_table(conn):
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(CREATE_TEMP_TABLE_SQL)
        cur.execute(TRUNCATE_TEMP_SQL)
    logger.info("Tabela dw2inst.temp_fatoauxiliar pronta (criada/truncada)")


# ---------------------------------------------------------------------------
# Bulk COPY via psycopg2 — muito mais rápido que INSERT
# ---------------------------------------------------------------------------

def _bulk_copy_to_postgres(pandas_df, conn):
    import csv
    logger.info(f"Iniciando COPY para dw2inst.temp_fatoauxiliar ({len(pandas_df):,} linhas)...")
    t0 = time.time()

    cols_present = [c for c in FATO_COLUMNS if c in pandas_df.columns]
    buf = io.StringIO()
    pandas_df[cols_present].to_csv(
        buf,
        index=False,
        header=False,
        na_rep="\\N",
        quoting=csv.QUOTE_MINIMAL,
        date_format="%Y-%m-%d %H:%M:%S",
    )
    buf.seek(0)

    cols_sql = ", ".join(cols_present)
    copy_sql = f"COPY dw2inst.temp_fatoauxiliar ({cols_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()

    elapsed = time.time() - t0
    logger.info(f"COPY concluído em {elapsed:.1f}s — {len(pandas_df):,} linhas gravadas")


# ---------------------------------------------------------------------------
# Spark session (leve — apenas leitura + dedup)
# ---------------------------------------------------------------------------

def _build_spark():
    from pyspark.sql import SparkSession

    spark_local_dirs = (
        os.environ.get("SPARK_LOCAL_DIRS")
        or os.environ.get("SPARK_LOCAL_DIR")
        or "/opt/parquet/tmp/spark_local"
    )
    os.makedirs(spark_local_dirs, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("rebuild_fato_auxiliar_2025")
        .config("spark.local.dir", spark_local_dirs)
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.instances", "2")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.default.parallelism", "16")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Passo 1: Descobre metas e coleta id_processo + campos base
# ---------------------------------------------------------------------------

def _discover_metas(base_path: str) -> list:
    found = []
    for d in sorted(os.listdir(base_path)):
        current = os.path.join(base_path, d, "current")
        if os.path.isdir(current) and d.startswith("meta_"):
            found.append(d.replace("meta_", ""))
    if not found:
        logger.warning(f"Nenhuma pasta meta_NN encontrada em {base_path}; usando fallback {ALL_METAS}")
        return ALL_METAS
    logger.info(f"Metas descobertas: {found}")
    return found


def _collect_ids_and_base(spark, metas: list):
    from pyspark.sql.functions import col, lit, first, max as spark_max

    all_frames = []

    for meta_num in metas:
        current_path = os.path.join(PRATA_EJUD_2025, f"meta_{meta_num}", "current")
        if not os.path.isdir(current_path):
            logger.warning(f"  Pasta não encontrada: {current_path} — pulando")
            continue

        parquet_files = sorted(glob.glob(os.path.join(current_path, "*.parquet")))
        if not parquet_files:
            logger.warning(f"  Nenhum parquet em {current_path} — pulando")
            continue

        logger.info(f"  meta_{meta_num}: {len(parquet_files)} arquivo(s) — {[os.path.basename(f) for f in parquet_files]}")

        for pfile in parquet_files:
            try:
                df = spark.read.parquet(pfile)
                if "coddoc" in df.columns and "id_processo" not in df.columns:
                    df = df.withColumnRenamed("coddoc", "id_processo")
                if "id_processo" not in df.columns:
                    logger.warning(f"    {os.path.basename(pfile)}: sem id_processo/coddoc — pulando")
                    continue

                df = df.withColumn("id_processo", col("id_processo").cast("string"))

                mappings = {
                    "dth_prim_distribuicao": ["dth_prim_distribuicao"],
                    "dth_ult_distribuicao":  ["dth_ult_distribuicao", "dthrmov"],
                    "cod_prim_oj":           ["cod_prim_oj", "codorgjulg"],
                    "cod_ult_oj":            ["cod_ult_oj", "cod_orgao_julgador", "cod_oj"],
                    "cod_prim_magistrado":   ["cod_prim_magistrado"],
                    "cod_ult_magistrado":    ["cod_ult_magistrado", "cod_magistrado", "codmagrel"],
                    "id_sigilo":             ["id_sigilo", "nivel_sigilo"],
                }

                select_exprs = [col("id_processo")]
                for target, candidates in mappings.items():
                    src = next((c for c in candidates if c in df.columns), None)
                    select_exprs.append(col(src).alias(target) if src else lit(None).alias(target))

                all_frames.append(df.select(*select_exprs))
                logger.info(f"    {os.path.basename(pfile)}: OK")
            except Exception as e:
                logger.warning(f"    Erro lendo {pfile}: {e}")

    if not all_frames:
        return None

    logger.info(f"Unificando {len(all_frames)} DataFrames...")
    combined = all_frames[0]
    for f in all_frames[1:]:
        combined = combined.unionByName(f, allowMissingColumns=True)

    # Exclude rows with dth_ult_distribuicao beyond the target year so that
    # cod_ult_magistrado / cod_ult_oj never reflect movements from a future year.
    from pyspark.sql.functions import lit as _lit
    _cutoff_ts = _lit(f"{META_ANO_TARGET}-12-31 23:59:59").cast("timestamp")
    combined = combined.filter(
        col("dth_ult_distribuicao").isNull() | (col("dth_ult_distribuicao") <= _cutoff_ts)
    )

    base_df = combined.groupBy("id_processo").agg(
        first(col("dth_prim_distribuicao"), ignorenulls=True).alias("dth_prim_distribuicao"),
        spark_max(col("dth_ult_distribuicao")).alias("dth_ult_distribuicao"),
        first(col("cod_prim_oj"),          ignorenulls=True).alias("cod_prim_oj"),
        first(col("cod_ult_oj"),           ignorenulls=True).alias("cod_ult_oj"),
        first(col("cod_prim_magistrado"),  ignorenulls=True).alias("cod_prim_magistrado"),
        first(col("cod_ult_magistrado"),   ignorenulls=True).alias("cod_ult_magistrado"),
        first(col("id_sigilo"),            ignorenulls=True).alias("id_sigilo"),
    )
    return base_df


# ---------------------------------------------------------------------------
# Passo 2: Enriquece com t_processo
# ---------------------------------------------------------------------------

def _read_parquet_arrow(path, columns=None):
    """
    Lê parquets com PyArrow (suporta TIMESTAMP(NANOS) que o PySpark rejeita).
    Converte colunas timestamp para us (microsegundos) — pandas/spark-compatível.
    Retorna um pandas DataFrame.
    """
    import pyarrow.parquet as pq
    import pyarrow as pa

    dataset = pq.ParquetDataset(path, use_legacy_dataset=False)
    table = dataset.read(columns=columns)

    # Converte qualquer coluna timestamp de nanos → micros para compatibilidade
    new_fields = []
    casts_needed = []
    for i, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            new_fields.append(field.with_type(pa.timestamp("us", tz=field.type.tz)))
            casts_needed.append((i, pa.timestamp("us", tz=field.type.tz)))
        else:
            new_fields.append(field)

    if casts_needed:
        arrays = list(table.columns)
        for idx, target_type in casts_needed:
            arrays[idx] = table.column(idx).cast(target_type)
        table = pa.table({f.name: arrays[i] for i, f in enumerate(new_fields)})

    return table.to_pandas()


def _enrich_from_t_processo(spark, base_df):
    from pyspark.sql.functions import col, coalesce, lit, to_timestamp

    if not os.path.exists(BRONZE_T_PROCESSO):
        logger.warning(f"t_processo não encontrado em {BRONZE_T_PROCESSO} — pulando")
        return base_df

    logger.info(f"Lendo t_processo via PyArrow (evita TIMESTAMP_NANOS): {BRONZE_T_PROCESSO}")
    try:
        # PyArrow lê e converte nanos→micros; pandas recebe timestamps python-compatíveis
        pdf = _read_parquet_arrow(BRONZE_T_PROCESSO)
        available = set(pdf.columns)
        logger.info(f"Colunas disponíveis em t_processo: {sorted(available)}")

        # ID do processo
        id_col = next((c for c in ["coddoc", "id_processo", "num_processo"] if c in available), None)
        if id_col is None:
            logger.warning("t_processo: nenhuma coluna de ID encontrada — pulando enriquecimento")
            return base_df

        # Data da primeira distribuição / autuação
        dth_prim_col = next((c for c in ["dthdistrib", "dtautua", "dtentr", "dth_distribuicao", "dth_autuacao"] if c in available), None)
        # Data da última distribuição
        dth_ult_col  = next((c for c in ["dthultdis", "dthultdistrib", "dth_ult_distribuicao", "dthdistrib", "dtautua"] if c in available and c != dth_prim_col), None) or dth_prim_col
        # Órgão julgador
        oj_col  = next((c for c in ["codorgjulg", "cod_orgao_julgador", "codoj"] if c in available), None)
        # Magistrado
        mag_col = next((c for c in ["codmagrel", "cod_magistrado", "codmag"] if c in available), None)

        logger.info(f"t_processo mapeamento: id={id_col}, dth_prim={dth_prim_col}, dth_ult={dth_ult_col}, oj={oj_col}, mag={mag_col}")

        # Seleciona apenas as colunas necessárias e renomeia
        keep = {id_col: "tp_id"}
        if dth_prim_col: keep[dth_prim_col] = "tp_dth_prim"
        if dth_ult_col and dth_ult_col != dth_prim_col: keep[dth_ult_col] = "tp_dth_ult"
        if oj_col:  keep[oj_col]  = "tp_cod_prim_oj"
        if mag_col: keep[mag_col] = "tp_cod_prim_mag"

        pdf_small = pdf[[c for c in keep]].rename(columns=keep).copy()
        pdf_small["tp_id"] = pdf_small["tp_id"].astype(str)

        # Converte colunas de data que possam estar como int (epoch ms ou s)
        for alias in ["tp_dth_prim", "tp_dth_ult"]:
            if alias not in pdf_small.columns:
                pdf_small[alias] = None
                continue
            if pdf_small[alias].dtype.kind in ("i", "u", "f"):
                import pandas as pd
                # detecta: se mediana > 1e12 → milissegundos, senão segundos
                median_val = pdf_small[alias].dropna().median()
                if median_val and median_val > 1_000_000_000_000:
                    pdf_small[alias] = pd.to_datetime(pdf_small[alias], unit="ms", utc=False, errors="coerce")
                else:
                    pdf_small[alias] = pd.to_datetime(pdf_small[alias], unit="s",  utc=False, errors="coerce")

        # Garante int para colunas de código
        for alias in ["tp_cod_prim_oj", "tp_cod_prim_mag"]:
            if alias not in pdf_small.columns:
                pdf_small[alias] = None
            else:
                pdf_small[alias] = pdf_small[alias].round().astype("Int64")

        # Dedup por id
        pdf_small = pdf_small.drop_duplicates(subset=["tp_id"])

        logger.info(f"t_processo lido via pandas: {len(pdf_small):,} linhas")

        # Cria Spark DataFrame a partir do pandas
        tp = spark.createDataFrame(pdf_small)

        # Garante que as colunas opcionais existam
        for c_need in ["tp_dth_prim", "tp_dth_ult", "tp_cod_prim_oj", "tp_cod_prim_mag"]:
            if c_need not in tp.columns:
                tp = tp.withColumn(c_need, lit(None).cast("timestamp" if "dth" in c_need else "int"))

        enriched = (
            base_df.join(tp, base_df["id_processo"] == tp["tp_id"], "left")
            .withColumn("dth_prim_distribuicao", coalesce(col("dth_prim_distribuicao"), col("tp_dth_prim")))
            .withColumn("dth_ult_distribuicao",  coalesce(col("dth_ult_distribuicao"),  col("tp_dth_ult")))
            .withColumn("cod_prim_oj",           coalesce(col("cod_prim_oj"),           col("tp_cod_prim_oj").cast("int")))
            .withColumn("cod_prim_magistrado",   coalesce(col("cod_prim_magistrado"),   col("tp_cod_prim_mag").cast("int")))
            .drop("tp_id", "tp_dth_prim", "tp_dth_ult", "tp_cod_prim_oj", "tp_cod_prim_mag")
        )
        logger.info("Enriquecimento com t_processo concluído")
        return enriched

    except Exception as e:
        logger.warning(f"t_processo: erro no enriquecimento ({e}) — continuando sem ele")
        return base_df


# ---------------------------------------------------------------------------
# Passo 3: Último evento
# ---------------------------------------------------------------------------

def _safe_int_col(c):
    """Converte coluna para int tratando float gravado como int (ex: 1.0 → 1)."""
    from pyspark.sql.functions import col, round as spark_round
    from pyspark.sql.types import FloatType, DoubleType
    # usamos round() antes do cast para garantir que 1.0 → 1 e não trunca
    return spark_round(col(c)).cast("int")


def _get_last_event(spark, base_df):
    from pyspark.sql.functions import col, lit, row_number, to_timestamp
    from pyspark.sql.window import Window

    ids = base_df.select("id_processo").distinct()
    frames = []

    for label, path in [("tab_xml_andamento", PRATA_TAB_XML), ("movimentorelator", BRONZE_MOVRELATOR)]:
        if not os.path.exists(path):
            logger.warning(f"  {label}: não encontrado em {path}")
            continue
        try:
            raw = spark.read.option("recursiveFileLookup", "true").parquet(path)
            available = set(raw.columns)
            logger.info(f"  {label}: colunas={sorted(available)}")

            # Coluna de ID do processo
            id_col = next((c for c in ["coddoc", "id_processo", "num_processo"] if c in available), None)
            if id_col is None:
                logger.warning(f"  {label}: sem coluna de ID — pulando")
                continue

            # Coluna de timestamp do evento
            ts_col = next((c for c in ["dthrmov", "dth_movimento", "dth_evento", "dthr_mov"] if c in available), None)
            if ts_col is None:
                logger.warning(f"  {label}: sem coluna de data/hora — pulando")
                continue

            # Coluna de código do tipo de movimento (opcional)
            cod_col = next((c for c in ["cod_tip_mov", "codtipmov", "cod_evento", "cod_movimento"] if c in available), None)

            # Coluna de complemento (opcional)
            comp_col = next((c for c in ["complemento", "complement", "descricao"] if c in available), None)

            logger.info(f"  {label}: id={id_col}, ts={ts_col}, cod={cod_col}, comp={comp_col}")

            select_exprs = [
                col(id_col).cast("string").alias("ev_id"),
                to_timestamp(col(ts_col)).alias("dthrmov"),
                (_safe_int_col(cod_col).alias("cod_tip_mov") if cod_col else lit(None).cast("int").alias("cod_tip_mov")),
                (col(comp_col).cast("string").alias("complemento") if comp_col else lit(None).cast("string").alias("complemento")),
            ]

            df = (
                raw.join(ids, col(id_col).cast("string") == col("id_processo"), "semi")
                .select(*select_exprs)
            )
            frames.append(df)
            logger.info(f"  {label}: OK")
        except Exception as e:
            logger.warning(f"  {label}: erro ({e})")

    if not frames:
        logger.warning("Nenhuma fonte de eventos — dth_ult_evento ficará NULL")
        return (
            base_df
            .withColumn("dth_ult_evento",    lit(None).cast("timestamp"))
            .withColumn("cod_ult_evento_cnj", lit(None).cast("int"))
            .withColumn("complemento",        lit(None).cast("string"))
        )

    eventos = frames[0]
    for f in frames[1:]:
        eventos = eventos.unionByName(f, allowMissingColumns=True)

    win = Window.partitionBy("ev_id").orderBy(
        (col("cod_tip_mov").isNotNull()).desc(),
        col("dthrmov").desc_nulls_last()
    )
    evt_top = (
        eventos
        .withColumn("_rn", row_number().over(win))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select(
            col("ev_id"),
            col("dthrmov").alias("dth_ult_evento"),
            col("cod_tip_mov").alias("cod_ult_evento_cnj"),
            col("complemento"),
        )
    )

    return (
        base_df.join(evt_top, base_df["id_processo"] == evt_top["ev_id"], "left")
        .drop("ev_id")
    )


# ---------------------------------------------------------------------------
# Passo 4: Indicadores baixado / julgado
# ---------------------------------------------------------------------------

def _get_indicadores(spark, base_df, metas: list):
    from pyspark.sql.functions import col, lit, when

    baixa_frames, julgado_frames = [], []

    for meta_num in metas:
        current_path = os.path.join(PRATA_EJUD_2025, f"meta_{meta_num}", "current")
        if not os.path.isdir(current_path):
            continue
        for pfile in glob.glob(os.path.join(current_path, f"baixm{meta_num}.parquet")):
            try:
                baixa_frames.append(
                    spark.read.parquet(pfile).select(col("coddoc").cast("string").alias("coddoc"))
                )
                logger.info(f"  baixm{meta_num}.parquet: encontrado")
            except Exception as e:
                logger.warning(f"  baixm{meta_num}.parquet: erro ({e})")
        for pfile in glob.glob(os.path.join(current_path, f"julgadom{meta_num}.parquet")):
            try:
                julgado_frames.append(
                    spark.read.parquet(pfile).select(col("coddoc").cast("string").alias("coddoc"))
                )
                logger.info(f"  julgadom{meta_num}.parquet: encontrado")
            except Exception as e:
                logger.warning(f"  julgadom{meta_num}.parquet: erro ({e})")

    enriched = base_df

    if baixa_frames:
        baixa_union = baixa_frames[0]
        for f in baixa_frames[1:]:
            baixa_union = baixa_union.union(f)
        baixa_union = baixa_union.distinct().withColumn("ind_baixado", lit(1).cast("short"))
        enriched = (
            enriched.join(baixa_union, enriched["id_processo"] == baixa_union["coddoc"], "left")
            .drop("coddoc")
            .withColumn("ind_baixado", when(col("ind_baixado").isNotNull(), col("ind_baixado")).otherwise(lit(0).cast("short")))
        )
    else:
        logger.warning("Nenhum baixm*.parquet encontrado — ind_baixado=0")
        enriched = enriched.withColumn("ind_baixado", lit(0).cast("short"))

    if julgado_frames:
        julgado_union = julgado_frames[0]
        for f in julgado_frames[1:]:
            julgado_union = julgado_union.union(f)
        julgado_union = julgado_union.distinct().withColumn("ind_julgado", lit(1).cast("short"))
        enriched = (
            enriched.join(julgado_union, enriched["id_processo"] == julgado_union["coddoc"], "left")
            .drop("coddoc")
            .withColumn("ind_julgado", when(col("ind_julgado").isNotNull(), col("ind_julgado")).otherwise(lit(0).cast("short")))
        )
    else:
        logger.warning("Nenhum julgadom*.parquet encontrado — ind_julgado=0")
        enriched = enriched.withColumn("ind_julgado", lit(0).cast("short"))

    return enriched


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def build_fato_2025(skip_postgres: bool, skip_parquet: bool, dry_run: bool):
    from pyspark.sql.functions import col, lit, trim, regexp_replace, coalesce, row_number
    from pyspark.sql.window import Window

    logger.info("=" * 70)
    logger.info(" REBUILD FATO_AUXILIAR 2025 — FONTE: prata/ejud/meta_cnj2025 ")
    logger.info("=" * 70)
    logger.info(f"Fonte prata   : {PRATA_EJUD_2025}")
    logger.info(f"Parquet limpo : {FATO_CLEAN_PATH}")
    logger.info(f"meta_ano alvo : {META_ANO_TARGET}")
    logger.info(f"dry_run       : {dry_run}")

    if not os.path.exists(PRATA_EJUD_2025):
        logger.error(f"Pasta prata não encontrada: {PRATA_EJUD_2025}")
        sys.exit(1)

    spark = _build_spark()

    try:
        # 1. Descobre metas
        metas = _discover_metas(PRATA_EJUD_2025)

        # 2. Coleta id_processo + campos base
        logger.info("\nPasso 1/4: Lendo variáveis prata e coletando id_processo...")
        base_df = _collect_ids_and_base(spark, metas)
        if base_df is None:
            logger.error("Falha ao ler parquets da prata.")
            sys.exit(1)

        total_ids = base_df.count()
        logger.info(f"Total id_processo únicos (todas as metas): {total_ids:,}")
        if total_ids == 0:
            logger.error("Nenhum id_processo encontrado na prata!")
            sys.exit(1)

        # 3. Enriquece com t_processo
        logger.info("\nPasso 2/4: Enriquecendo com t_processo (distribuição)...")
        base_df = _enrich_from_t_processo(spark, base_df)

        # 4. Último evento
        logger.info("\nPasso 3/4: Último evento (tab_xml_andamento + movimentorelator)...")
        base_df = _get_last_event(spark, base_df)

        # 5. Indicadores baixado/julgado
        logger.info("\nPasso 4/4: Indicadores baixado/julgado...")
        base_df = _get_indicadores(spark, base_df, metas)

        # 6. Finaliza colunas
        base_df = base_df.withColumn("meta_ano", lit(META_ANO_TARGET).cast("int"))
        for mc in FATO_COLUMNS:
            if mc not in base_df.columns:
                logger.warning(f"  Coluna ausente, adicionando como NULL: {mc}")
                base_df = base_df.withColumn(mc, lit(None))

        # Garante tipos corretos — colunas int/long podem vir como float da prata
        from pyspark.sql.functions import round as spark_round
        _int_cols  = ["cod_prim_oj", "cod_ult_oj", "cod_prim_magistrado", "cod_ult_magistrado",
                      "cod_ult_evento_cnj", "meta_ano"]
        _long_cols = ["id_sigilo"]
        for _c in _int_cols:
            if _c in base_df.columns:
                base_df = base_df.withColumn(_c, spark_round(col(_c)).cast("int"))
        for _c in _long_cols:
            if _c in base_df.columns:
                base_df = base_df.withColumn(_c, spark_round(col(_c)).cast("long"))

        base_df = base_df.select(*FATO_COLUMNS)

        for c, t in base_df.dtypes:
            if "string" in t:
                base_df = base_df.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))

        # Dedup final de segurança
        order_ts = coalesce(col("dth_ult_evento"), col("dth_ult_distribuicao"), col("dth_prim_distribuicao"))
        win = Window.partitionBy("id_processo").orderBy(order_ts.desc_nulls_last())
        final = (
            base_df
            .withColumn("_rn", row_number().over(win))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .filter(col("id_processo").isNotNull())
        )

        total_final = final.count()
        logger.info(f"\nTotal final: {total_final:,} linhas únicas")

        if dry_run:
            logger.info("\nDRY RUN — nenhuma gravação será feita.")
            logger.info("Amostra (10 linhas):")
            final.show(10, truncate=False)
            logger.info("Nulls por coluna:")
            from pyspark.sql.functions import count, when
            final.select([count(when(col(c).isNull(), c)).alias(c) for c in final.columns]).show()
            return

        # 7. Reescreve parquet limpo
        if not skip_parquet:
            logger.info(f"\nReescrevendo parquet limpo em: {FATO_CLEAN_PATH}")
            t0 = time.time()
            final.coalesce(4).write.mode("overwrite").option("compression", "snappy").parquet(FATO_CLEAN_PATH)
            elapsed = time.time() - t0
            out_files = sorted(glob.glob(os.path.join(FATO_CLEAN_PATH, "*.parquet")))
            total_mb = sum(os.path.getsize(f) for f in out_files) / 1024 / 1024
            logger.info(f"Parquet reescrito em {elapsed:.1f}s — {len(out_files)} arquivo(s), {total_mb:.1f} MB")

        # 8. Grava no PostgreSQL via COPY
        if not skip_postgres:
            logger.info("\nCarga no PostgreSQL via COPY...")
            t0 = time.time()
            pandas_df = final.coalesce(4).toPandas()
            logger.info(f"Conversão pandas: {time.time()-t0:.1f}s — {len(pandas_df):,} linhas, {pandas_df.memory_usage(deep=True).sum()/1024**2:.1f} MB")

            conn = _get_pg_connection()
            try:
                _ensure_temp_table(conn)
                _bulk_copy_to_postgres(pandas_df, conn)
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM dw2inst.temp_fatoauxiliar")
                    cnt_final = cur.fetchone()[0]
                logger.info(f"Verificação final: {cnt_final:,} linhas em dw2inst.temp_fatoauxiliar")
            finally:
                conn.close()

        logger.info("\n" + "=" * 70)
        logger.info("REBUILD CONCLUÍDO COM SUCESSO")
        logger.info(f"   Registros únicos 2025  : {total_final:,}")
        if not skip_parquet:
            logger.info(f"   Parquet limpo          : {FATO_CLEAN_PATH}")
        if not skip_postgres:
            logger.info(f"   Tabela PostgreSQL      : dw2inst.temp_fatoauxiliar")
        logger.info("=" * 70)

    finally:
        spark.stop()
        logger.info("Spark encerrado.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Reconstrói fato_auxiliar 2025 a partir de prata/ejud/meta_cnj2025")
    parser.add_argument("--skip-postgres", action="store_true", help="Não grava no PostgreSQL")
    parser.add_argument("--skip-parquet",  action="store_true", help="Não reescreve o parquet")
    parser.add_argument("--dry-run",       action="store_true", help="Apenas conta, não grava nada")
    args = parser.parse_args()

    build_fato_2025(
        skip_postgres=args.skip_postgres,
        skip_parquet=args.skip_parquet,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
