#!/usr/bin/env python3
"""
rebuild_fato_auxiliar_2025_eproc2g.py
===========================================
Reconstrói a fato_auxiliar para o ano 2025 — sistema EPROC2G.

Fonte principal: /opt/parquet/data/prata/eproc2g/meta_cnj2025/meta_01/current/*.parquet

Parquets disponíveis:
  cnm01.parquet, julgadom01.parquet, baixm01.parquet, desm01.parquet,
  susm01.parquet, mpum01.parquet, primeirasentm01.parquet

Passos:
  1. Lê todos os parquets de meta_01/current (auto-descobre metas disponíveis)
  2. Coleta id_processo único de todas as variáveis
  3. Enriquece com datas via bronze/eproc2g/historico_distribuicao_judicial
  4. Obtém último evento via bronze/eproc2g/movimento
  5. Obtém ind_baixado (baixm*.parquet) e ind_julgado (julgadom*.parquet)
  6. Deduplica por id_processo
  7. APPENDA em dw2inst.temp_fatoauxiliar (não trunca — EJUD já populou)
  8. Reescreve parquet limpo em ouro/fato_auxiliar_eproc2g_2025_clean

ORDEM DE EXECUÇÃO RECOMENDADA:
  1. python rebuild_fato_auxiliar_2025.py          # EJUD — cria/trunca a tabela
  2. python rebuild_fato_auxiliar_2025_eproc2g.py  # EPROC2G — appenda

Uso no servidor:
  cd /opt/parquet/projetos/2inst_meta_relatorio
  python rebuild_fato_auxiliar_2025_eproc2g.py [--skip-postgres] [--skip-parquet] [--dry-run] [--truncate-first]

Flags opcionais:
  --skip-postgres    Pula a gravação no PostgreSQL
  --skip-parquet     Pula a reescrita do parquet limpo
  --dry-run          Apenas mostra contagens, não grava nada
  --truncate-first   Trunca temp_fatoauxiliar antes de inserir (use se rodar este script sozinho)
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
        logging.FileHandler(f"/tmp/rebuild_fato_auxiliar_2025_eproc2g_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
logger = logging.getLogger("rebuild_fato_auxiliar_2025_eproc2g")

# ---------------------------------------------------------------------------
# Caminhos — EPROC2G
# ---------------------------------------------------------------------------
PRATA_EPROC_2025    = os.path.join(PARQUET_HOME, "data", "prata", "eproc2g", "meta_cnj2025")
BRONZE_EPROC_HDJ    = os.path.join(PARQUET_HOME, "data", "bronze", "eproc2g", "historico_distribuicao_judicial")
BRONZE_EPROC_MOV    = os.path.join(PARQUET_HOME, "data", "bronze", "eproc2g", "movimento")
BRONZE_EPROC_PROC   = os.path.join(PARQUET_HOME, "data", "bronze", "eproc2g", "processo", "processo.parquet")
FATO_CLEAN_PATH     = os.path.join(PARQUET_HOME, "data", "ouro", "fato_auxiliar_eproc2g_2025_clean")
META_ANO_TARGET     = 2025
SISTEMA             = "eproc2g"  # gravado como literal na tabela (para rastreabilidade)

# Lista de metas (fallback — a descoberta automática tem prioridade)
ALL_METAS = ["01"]

# Colunas da tabela fato_auxiliar (ordem importa para o COPY)
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

# DDL da tabela temporária (idêntico ao script EJUD — IF NOT EXISTS é seguro)
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
# Helpers de conexão PostgreSQL
# ---------------------------------------------------------------------------

def _get_pg_connection():
    import psycopg2
    pg_host = os.environ.get("POSTGRESQL_SL_HOST", os.environ.get("PG_HOST", "172.20.30.141"))
    pg_port = int(os.environ.get("POSTGRESQL_SL_PORT", os.environ.get("PG_PORT", "5432")))
    pg_db   = os.environ.get("POSTGRESQL_SL_DBNAME", os.environ.get("PG_DB", "postgres"))
    pg_user = os.environ.get("POSTGRESQL_SL_USER", os.environ.get("PG_USER", "felipe.lisboa"))
    pg_pass = os.environ.get("POSTGRESQL_SL_PASSWORD", os.environ.get("PG_PASS", ""))
    return psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pass)


def _prepare_table(conn, truncate_first: bool):
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(CREATE_TEMP_TABLE_SQL)
        if truncate_first:
            cur.execute(TRUNCATE_TEMP_SQL)
            logger.info("Tabela dw2inst.temp_fatoauxiliar truncada (--truncate-first)")
        else:
            logger.info("Tabela dw2inst.temp_fatoauxiliar pronta (append mode — sem truncar)")


# ---------------------------------------------------------------------------
# Bulk COPY via psycopg2
# ---------------------------------------------------------------------------

def _bulk_copy_to_postgres(pandas_df, conn):
    import csv
    logger.info(f"Iniciando COPY (append) para dw2inst.temp_fatoauxiliar ({len(pandas_df):,} linhas)...")
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
    logger.info(f"COPY concluído em {elapsed:.1f}s — {len(pandas_df):,} linhas inseridas")


# ---------------------------------------------------------------------------
# Spark session
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
        .appName("rebuild_fato_auxiliar_2025_eproc2g")
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
    """
    Lê todos os parquets de prata/eproc2g e retorna DataFrame agrupado por id_processo.
    Nomes de colunas do EPROC2G já são normalizados (id_processo, cod_orgao_julgador, etc.)
    pois o pipeline eproc2g já os renomeia antes de salvar na prata.
    """
    from pyspark.sql.functions import col, lit, first, max as spark_max

    all_frames = []

    for meta_num in metas:
        current_path = os.path.join(PRATA_EPROC_2025, f"meta_{meta_num}", "current")
        if not os.path.isdir(current_path):
            logger.warning(f"  Pasta não encontrada: {current_path} — pulando")
            continue

        parquet_files = sorted(glob.glob(os.path.join(current_path, "*.parquet")))
        if not parquet_files:
            logger.warning(f"  Nenhum parquet em {current_path} — pulando")
            continue

        logger.info(f"  meta_{meta_num}: {len(parquet_files)} arquivo(s) — {[os.path.basename(f) for f in parquet_files]}")

        for pfile in parquet_files:
            fname = os.path.basename(pfile)
            # Pula arquivos de indicadores — serão tratados em _get_indicadores
            if fname.startswith("baixm") or fname.startswith("julgadom"):
                logger.info(f"    {fname}: reservado para indicadores — pulando nesta etapa")
                continue
            try:
                df = spark.read.parquet(pfile)

                # EPROC2G pode usar id_processo ou coddoc
                if "coddoc" in df.columns and "id_processo" not in df.columns:
                    df = df.withColumnRenamed("coddoc", "id_processo")
                if "id_processo" not in df.columns:
                    logger.warning(f"    {fname}: sem id_processo/coddoc — pulando")
                    continue

                df = df.withColumn("id_processo", col("id_processo").cast("string"))

                # Mapeamento EPROC2G: os parquets prata já têm nomes normalizados
                # mas mantemos candidatos alternativos para robustez
                mappings = {
                    "dth_prim_distribuicao": ["dth_prim_distribuicao", "dth_distribuicao", "dth_autuacao"],
                    "dth_ult_distribuicao":  ["dth_ult_distribuicao", "dthrmov", "dth_distribuicao"],
                    "cod_prim_oj":           ["cod_prim_oj", "cod_orgao_julgador", "codorgjulg"],
                    "cod_ult_oj":            ["cod_ult_oj", "cod_orgao_julgador", "codorgjulg"],
                    "cod_prim_magistrado":   ["cod_prim_magistrado", "cod_magistrado"],
                    "cod_ult_magistrado":    ["cod_ult_magistrado", "cod_magistrado"],
                    "id_sigilo":             ["id_sigilo", "nivel_sigilo", "sigilo"],
                }

                select_exprs = [col("id_processo")]
                for target, candidates in mappings.items():
                    src = next((c for c in candidates if c in df.columns), None)
                    select_exprs.append(col(src).alias(target) if src else lit(None).alias(target))

                all_frames.append(df.select(*select_exprs))
                logger.info(f"    {fname}: OK — colunas usadas: {[e._jc.toString() if hasattr(e, '_jc') else str(e) for e in select_exprs[:3]]}...")
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

    # Agrupa por id_processo consolidando campos de múltiplas variáveis
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
# Passo 2: Enriquece com historico_distribuicao_judicial ou processo
# ---------------------------------------------------------------------------

def _enrich_from_eproc_bronze(spark, base_df):
    from pyspark.sql.functions import col, coalesce, to_timestamp

    enriched = base_df

    # Tenta historico_distribuicao_judicial primeiro (mais detalhado)
    if os.path.exists(BRONZE_EPROC_HDJ):
        try:
            logger.info(f"Lendo historico_distribuicao_judicial de: {BRONZE_EPROC_HDJ}")
            hdj = (
                spark.read.option("recursiveFileLookup", "true").parquet(BRONZE_EPROC_HDJ)
                .select(
                    col("id_processo").cast("string").alias("hdj_id"),
                    to_timestamp(col("dth_distribuicao")).alias("hdj_dth_prim"),
                    col("cod_orgao_julgador").cast("int").alias("hdj_cod_oj"),
                    col("cod_magistrado").cast("int").alias("hdj_cod_mag"),
                )
            )
            # Prim distribuição = min, ult distribuição = max por processo
            from pyspark.sql.functions import min as spark_min, max as spark_max
            from pyspark.sql.functions import first
            hdj_agg = (
                hdj.groupBy("hdj_id").agg(
                    spark_min("hdj_dth_prim").alias("hdj_dth_prim"),
                    spark_max("hdj_dth_prim").alias("hdj_dth_ult"),
                    first(col("hdj_cod_oj"),  ignorenulls=True).alias("hdj_cod_oj"),
                    first(col("hdj_cod_mag"), ignorenulls=True).alias("hdj_cod_mag"),
                )
            )

            enriched = (
                enriched.join(hdj_agg, enriched["id_processo"] == hdj_agg["hdj_id"], "left")
                .withColumn("dth_prim_distribuicao", coalesce(col("dth_prim_distribuicao"), col("hdj_dth_prim")))
                .withColumn("dth_ult_distribuicao",  coalesce(col("dth_ult_distribuicao"),  col("hdj_dth_ult")))
                .withColumn("cod_prim_oj",           coalesce(col("cod_prim_oj"),           col("hdj_cod_oj")))
                .withColumn("cod_prim_magistrado",   coalesce(col("cod_prim_magistrado"),   col("hdj_cod_mag")))
                .drop("hdj_id", "hdj_dth_prim", "hdj_dth_ult", "hdj_cod_oj", "hdj_cod_mag")
            )
            logger.info("Enriquecimento com historico_distribuicao_judicial concluído")
        except Exception as e:
            logger.warning(f"historico_distribuicao_judicial: erro ({e})")

    # Fallback: bronze/eproc2g/processo
    if os.path.exists(BRONZE_EPROC_PROC):
        try:
            logger.info(f"Lendo bronze/eproc2g/processo de: {BRONZE_EPROC_PROC}")
            proc = (
                spark.read.parquet(BRONZE_EPROC_PROC)
                .select(
                    col("id_processo").cast("string").alias("proc_id"),
                    to_timestamp(col("dth_distribuicao")).alias("proc_dth"),
                    col("cod_orgao_julgador").cast("int").alias("proc_cod_oj"),
                    col("cod_magistrado").cast("int").alias("proc_cod_mag"),
                )
                .dropDuplicates(["proc_id"])
            )
            enriched = (
                enriched.join(proc, enriched["id_processo"] == proc["proc_id"], "left")
                .withColumn("dth_prim_distribuicao", coalesce(col("dth_prim_distribuicao"), col("proc_dth")))
                .withColumn("dth_ult_distribuicao",  coalesce(col("dth_ult_distribuicao"),  col("proc_dth")))
                .withColumn("cod_prim_oj",           coalesce(col("cod_prim_oj"),           col("proc_cod_oj")))
                .withColumn("cod_prim_magistrado",   coalesce(col("cod_prim_magistrado"),   col("proc_cod_mag")))
                .drop("proc_id", "proc_dth", "proc_cod_oj", "proc_cod_mag")
            )
            logger.info("Enriquecimento com bronze/processo concluído")
        except Exception as e:
            logger.warning(f"bronze/eproc2g/processo: erro ({e})")

    return enriched


# ---------------------------------------------------------------------------
# Passo 3: Último evento (movimento EPROC2G)
# ---------------------------------------------------------------------------

def _get_last_event(spark, base_df):
    from pyspark.sql.functions import col, lit, row_number, to_timestamp
    from pyspark.sql.window import Window

    if not os.path.exists(BRONZE_EPROC_MOV):
        logger.warning(f"bronze/eproc2g/movimento não encontrado em {BRONZE_EPROC_MOV} — dth_ult_evento ficará NULL")
        return (
            base_df
            .withColumn("dth_ult_evento",    lit(None).cast("timestamp"))
            .withColumn("cod_ult_evento_cnj", lit(None).cast("int"))
            .withColumn("complemento",        lit(None).cast("string"))
        )

    ids = base_df.select("id_processo").distinct()

    try:
        logger.info(f"Lendo bronze/eproc2g/movimento de: {BRONZE_EPROC_MOV}")

        mov = spark.read.option("recursiveFileLookup", "true").parquet(BRONZE_EPROC_MOV)

        # EPROC2G movimento pode usar id_processo ou outro identificador
        id_col = None
        for candidate in ["id_processo", "coddoc", "num_processo"]:
            if candidate in mov.columns:
                id_col = candidate
                break

        if id_col is None:
            logger.warning(f"Coluna de ID não encontrada em movimento EPROC2G. Colunas: {mov.columns}")
            raise ValueError("ID column not found in eproc2g movimento")

        # Coluna de timestamp de movimento
        ts_col = next((c for c in ["dth_movimento", "dth_evento", "dthrmov", "dth"] if c in mov.columns), None)
        # Código do tipo de movimento
        cod_col = next((c for c in ["cod_movimento", "cod_tip_mov", "cod_evento"] if c in mov.columns), None)
        # Complemento
        comp_col = next((c for c in ["complemento", "descricao", "observacao"] if c in mov.columns), None)

        select_exprs = [col(id_col).cast("string").alias("ev_id")]
        select_exprs.append(to_timestamp(col(ts_col)).alias("dthrmov") if ts_col else lit(None).cast("timestamp").alias("dthrmov"))
        select_exprs.append(col(cod_col).cast("int").alias("cod_tip_mov") if cod_col else lit(None).cast("int").alias("cod_tip_mov"))
        select_exprs.append(col(comp_col).cast("string").alias("complemento") if comp_col else lit(None).cast("string").alias("complemento"))

        eventos = (
            mov.join(ids, col(id_col) == col("id_processo"), "semi")
            .select(*select_exprs)
        )

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

        result = (
            base_df.join(evt_top, base_df["id_processo"] == evt_top["ev_id"], "left")
            .drop("ev_id")
        )
        logger.info("Enriquecimento com movimento EPROC2G concluído")
        return result

    except Exception as e:
        logger.warning(f"Erro no enriquecimento com movimento EPROC2G: {e} — continuando sem dth_ult_evento")
        return (
            base_df
            .withColumn("dth_ult_evento",    lit(None).cast("timestamp"))
            .withColumn("cod_ult_evento_cnj", lit(None).cast("int"))
            .withColumn("complemento",        lit(None).cast("string"))
        )


# ---------------------------------------------------------------------------
# Passo 4: Indicadores baixado / julgado
# ---------------------------------------------------------------------------

def _get_indicadores(spark, base_df, metas: list):
    from pyspark.sql.functions import col, lit, when

    baixa_frames, julgado_frames = [], []

    for meta_num in metas:
        current_path = os.path.join(PRATA_EPROC_2025, f"meta_{meta_num}", "current")
        if not os.path.isdir(current_path):
            continue

        for pfile in glob.glob(os.path.join(current_path, f"baixm{meta_num}.parquet")):
            try:
                df = spark.read.parquet(pfile)
                id_col = "id_processo" if "id_processo" in df.columns else "coddoc"
                baixa_frames.append(df.select(col(id_col).cast("string").alias("coddoc")))
                logger.info(f"  baixm{meta_num}.parquet: encontrado ({id_col})")
            except Exception as e:
                logger.warning(f"  baixm{meta_num}.parquet: erro ({e})")

        for pfile in glob.glob(os.path.join(current_path, f"julgadom{meta_num}.parquet")):
            try:
                df = spark.read.parquet(pfile)
                id_col = "id_processo" if "id_processo" in df.columns else "coddoc"
                julgado_frames.append(df.select(col(id_col).cast("string").alias("coddoc")))
                logger.info(f"  julgadom{meta_num}.parquet: encontrado ({id_col})")
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

def build_fato_eproc_2025(skip_postgres: bool, skip_parquet: bool, dry_run: bool, truncate_first: bool):
    from pyspark.sql.functions import col, lit, trim, regexp_replace, coalesce, row_number
    from pyspark.sql.window import Window

    logger.info("=" * 70)
    logger.info(" REBUILD FATO_AUXILIAR 2025 — EPROC2G ")
    logger.info("=" * 70)
    logger.info(f"Fonte prata   : {PRATA_EPROC_2025}")
    logger.info(f"Parquet limpo : {FATO_CLEAN_PATH}")
    logger.info(f"meta_ano alvo : {META_ANO_TARGET}")
    logger.info(f"truncate_first: {truncate_first}")
    logger.info(f"dry_run       : {dry_run}")

    if not os.path.exists(PRATA_EPROC_2025):
        logger.error(f"Pasta prata EPROC2G não encontrada: {PRATA_EPROC_2025}")
        sys.exit(1)

    spark = _build_spark()

    try:
        # 1. Descobre metas
        metas = _discover_metas(PRATA_EPROC_2025)

        # 2. Coleta id_processo + campos base
        logger.info("\nPasso 1/4: Lendo variáveis prata EPROC2G...")
        base_df = _collect_ids_and_base(spark, metas)
        if base_df is None:
            logger.error("Falha ao ler parquets da prata EPROC2G.")
            sys.exit(1)

        total_ids = base_df.count()
        logger.info(f"Total id_processo únicos EPROC2G: {total_ids:,}")
        if total_ids == 0:
            logger.error("Nenhum id_processo encontrado na prata EPROC2G!")
            sys.exit(1)

        # 3. Enriquece com bronze EPROC2G
        logger.info("\nPasso 2/4: Enriquecendo com bronze EPROC2G (distribuição)...")
        base_df = _enrich_from_eproc_bronze(spark, base_df)

        # 4. Último evento
        logger.info("\nPasso 3/4: Último evento (bronze/eproc2g/movimento)...")
        base_df = _get_last_event(spark, base_df)

        # 5. Indicadores
        logger.info("\nPasso 4/4: Indicadores baixado/julgado...")
        base_df = _get_indicadores(spark, base_df, metas)

        # 6. Finaliza colunas
        base_df = base_df.withColumn("meta_ano", lit(META_ANO_TARGET).cast("int"))
        for mc in FATO_COLUMNS:
            if mc not in base_df.columns:
                logger.warning(f"  Coluna ausente, adicionando como NULL: {mc}")
                base_df = base_df.withColumn(mc, lit(None))
        base_df = base_df.select(*FATO_COLUMNS)

        for c, t in base_df.dtypes:
            if "string" in t:
                base_df = base_df.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))

        # Dedup final
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
        logger.info(f"\nTotal final EPROC2G: {total_final:,} linhas únicas")

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

        # 8. Grava no PostgreSQL via COPY (append)
        if not skip_postgres:
            logger.info("\nCarga no PostgreSQL via COPY (append)...")
            t0 = time.time()
            pandas_df = final.coalesce(4).toPandas()
            logger.info(f"Conversão pandas: {time.time()-t0:.1f}s — {len(pandas_df):,} linhas, {pandas_df.memory_usage(deep=True).sum()/1024**2:.1f} MB")

            conn = _get_pg_connection()
            try:
                _prepare_table(conn, truncate_first=truncate_first)
                _bulk_copy_to_postgres(pandas_df, conn)
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM dw2inst.temp_fatoauxiliar")
                    cnt_total = cur.fetchone()[0]
                logger.info(f"Total na tabela após insert EPROC2G: {cnt_total:,} linhas")
            finally:
                conn.close()

        logger.info("\n" + "=" * 70)
        logger.info("REBUILD EPROC2G CONCLUÍDO COM SUCESSO")
        logger.info(f"   Registros únicos EPROC2G 2025 : {total_final:,}")
        if not skip_parquet:
            logger.info(f"   Parquet limpo                 : {FATO_CLEAN_PATH}")
        if not skip_postgres:
            logger.info(f"   Tabela PostgreSQL             : dw2inst.temp_fatoauxiliar (append)")
        logger.info("=" * 70)

    finally:
        spark.stop()
        logger.info("Spark encerrado.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Reconstrói fato_auxiliar 2025 — EPROC2G — a partir de prata/eproc2g/meta_cnj2025"
    )
    parser.add_argument("--skip-postgres",   action="store_true", help="Não grava no PostgreSQL")
    parser.add_argument("--skip-parquet",    action="store_true", help="Não reescreve o parquet")
    parser.add_argument("--dry-run",         action="store_true", help="Apenas conta, não grava nada")
    parser.add_argument("--truncate-first",  action="store_true",
                        help="Trunca temp_fatoauxiliar antes de inserir (use se rodar este script isolado)")
    args = parser.parse_args()

    build_fato_eproc_2025(
        skip_postgres=args.skip_postgres,
        skip_parquet=args.skip_parquet,
        dry_run=args.dry_run,
        truncate_first=args.truncate_first,
    )


if __name__ == "__main__":
    main()
