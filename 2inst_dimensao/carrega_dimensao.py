#carrega_dimonsao.py
# -*- coding: utf-8 -*-
"""
Gera TODAS as dimensões do sistema EJUD a partir de um único JSON.
- Cria TempViews das fontes
- Executa a SQL de cada dimensão (e opcionalmente sql_extra)
- Normaliza, renomeia, aplica casts, defaults
- Dedup por PK com regra por dimensão
- Parquet único LOCAL e/ou S3 (por dimensão)
- (Opcional) carrega cada dimensão no Postgres
"""
import argparse
import os, sys, json, glob, shutil
from datetime import datetime
from dotenv import load_dotenv

# ==== ENV / PATHS ====
PARQUET_HOME = os.environ.get("PARQUET_HOME", "/opt/parquet")
#MODULO_PATH = os.path.join(PARQUET_HOME, "airflow_2inst", "scripts")
#if MODULO_PATH not in sys.path:
#    sys.path.insert(0, MODULO_PATH)

load_dotenv(dotenv_path=os.path.join(PARQUET_HOME, "airflow", "dotenv", ".env"))

from modulo.conexoes import get_postgres_connection
from modulo.controle_carga import registrar_inicio_carga, registrar_fim_carga

AMBIENTE    = os.environ.get("AMBIENTE", "local").lower()
STRICT_PK   = os.environ.get("STRICT_PK", "false").lower() in ("1","true","yes")
WRITE_LOCAL = os.environ.get("WRITE_LOCAL", "true").lower() in ("1","true","yes")
WRITE_TO_S3 = os.environ.get("WRITE_TO_S3", "false").lower() in ("1","true","yes")
LOAD_TO_DB  = os.environ.get("LOAD_TO_DB", "true").lower() in ("1","true","yes")

BUCKET_BRONZE = os.environ.get("BUCKET_BRONZE", "sgdai-data-lake")
BUCKET_PRATA  = os.environ.get("BUCKET_PRATA",  "sgdai-data-lake")
BUCKET_OURO = os.environ.get("BUCKET_OURO", "sgdai-data-lake")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze")
PRATA_PREFIX  = os.environ.get("PRATA_PREFIX",  "prata/dim2inst")
OURO_PREFIX   = os.environ.get("OURO_PREFIX",   "ouro/dim2inst")

BASE_DIR = os.path.join(PARQUET_HOME, "projetos", "dimensao")
CONFIG_JSON = os.path.join(BASE_DIR, "dimensoes.json")

# ==== LOGS ====
def log_b(t): print(f"\n🚩 ====> {t}\n")
def log_i(t): print(f"ℹ️  {t}")
def log_ok(t): print(f"✅ {t}")
def log_w(t): print(f"⚠️  {t}")
def log_e(t): print(f"❌ {t}")

# ==== Spark ====
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, from_utc_timestamp
from pyspark.sql.window import Window

def spark_session():
    b = (SparkSession.builder
            .appName("Dimensoes_EJUD")
            .config("spark.driver.memory", "24g")
            .config("spark.executor.memory", "32g")
            .config("spark.executor.cores", "16")
            .config("spark.driver.cores", "4")
            .config("spark.local.dir", "/opt/parquet/tmp_spark")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
            .config("spark.sql.shuffle.partitions", "64")
            .config("spark.memory.fraction", "0.8")
            .config("spark.executor.memoryOverhead", "4g")
            .config("spark.sql.parquet.enableVectorizedWriter", "true")
            .config("spark.sql.files.maxPartitionBytes", "128MB"))
    if AMBIENTE != "local":
        b = b.config("spark.hadoop.fs.s3a.fast.upload","true")
    return b.getOrCreate()

from pyspark.sql.functions import col, from_utc_timestamp
from pyspark.sql.types import TimestampType
try:
    from pyspark.sql.types import TimestampNTZType
except ImportError:
    TimestampNTZType = None

def apply_tz_conversion(df, cfg):
    tz = "America/Sao_Paulo"
    local_cfg = set([c.lower() for c in cfg.get("timestamps_local", [])])

    fields = {f.name: f.dataType for f in df.schema.fields}
    for name, dtype in fields.items():
        # só timestamps
        is_ts = isinstance(dtype, TimestampType) or (TimestampNTZType and isinstance(dtype, TimestampNTZType))
        if not is_ts:
            continue

        # se estiver configurado como local -> NÃO converter
        if name.lower() in local_cfg:
            continue

        # se for Timestamp NTZ (sem fuso) e não estiver explicitamente em UTC -> melhor não converter
        if TimestampNTZType and isinstance(dtype, TimestampNTZType):
            continue

        # caso geral: assumir UTC e converter para SP
        df = df.withColumn(name, from_utc_timestamp(col(name), tz))
    return df

# A função bronze_path foi substituída por data_path para ser mais genérica
# Defina esta nova função para lidar com diferentes camadas.
def data_path(layer, schema, tabela):
    """
    Constrói o caminho completo para o arquivo Parquet com base na camada.
    """
    if AMBIENTE == "local":
        return os.path.join(PARQUET_HOME, "data", layer, schema, tabela)
    
    # Se a camada for ouro, o bucket deve ser o ouro
    bucket = BUCKET_OURO if layer == "ouro" else BUCKET_PRATA if layer == "prata" else BUCKET_BRONZE
    prefix = OURO_PREFIX if layer == "ouro" else PRATA_PREFIX if layer == "prata" else BRONZE_PREFIX

    return f"s3a://{bucket}/{prefix}/{schema}/{tabela}"

# ==== Utils ====
def trava_execucao_duplicada(id_projeto: int, tabela_nome: str) -> bool:
    """
    Trava por (id_projeto, tabela_nome) para permitir execução paralela por dimensão.
    """
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                  FROM logs.execucao_carga_projeto
                 WHERE id_projeto = %s
                   AND tabela_nome = %s
                   AND status = 'INICIADA'
                   AND dt_fim_carga IS NULL
                 LIMIT 1
            """, (id_projeto, tabela_nome))
            return cur.fetchone() is None
    finally:
        conn.close()

def load_config():
    with open(CONFIG_JSON, "r") as f:
        return json.load(f)

def detecta_schema_qualificado_no_sql(sql_text: str, schemas: set) -> bool:
    low = sql_text.lower()
    return any((s.lower()+".") in low for s in schemas)

def rewrite_sql(sql_text: str, schemas: set) -> str:
    novo = sql_text
    for s in sorted(schemas, key=len, reverse=True):
        novo = novo.replace(f"{s}.", f"{s}_")
        novo = novo.replace(f"{s.upper()}.", f"{s.upper()}_")
    return novo

# FUNÇÃO AJUSTADA para ler de camadas bronze, prata ou ouro
def registra_views(spark, fontes, schemas):
    for item in fontes:
        path_schema = item["schema"]
        tabela = item["tabela"]
        alias = item.get("view", tabela)
        
        # Lógica para determinar a camada a partir do caminho no JSON
        if path_schema.startswith("prata/"):
            layer = "prata"
            schema_name = path_schema.split("/")[1]
            path = data_path(layer, schema_name, tabela)
        elif path_schema.startswith("ouro/"):
            layer = "ouro"
            schema_name = path_schema.split("/")[1]
            path = data_path(layer, schema_name, tabela)
        else:
            layer = "bronze"
            schema_name = path_schema
            path = data_path(layer, schema_name, tabela)

        try:
            log_i(f"-> {schema_name}.{tabela} ({path})")
            df = spark.read.option("pathGlobFilter", "*.parquet").parquet(path)
            
            # 1) nome simples da tabela física (ex.: orgaojulgador)
            df.createOrReplaceTempView(tabela)
            # 2) nome qualificado reescrito (ex.: ejud_orgaojulgador) para 'ejud.tabela'
            df.createOrReplaceTempView(f"{schema_name}_{tabela}")

            if alias != tabela:
                # 3) alias simples (ex.: processo)
                df.createOrReplaceTempView(alias)
                # 4) alias qualificado reescrito (ex.: ejud_processo) para 'ejud.processo'
                df.createOrReplaceTempView(f"{schema_name}_{alias}")
        except Exception as e:
            log_e(f"Erro ao carregar a fonte {path_schema}.{tabela}: {e}")
            raise e

def exec_sql(spark, sql_path: str, fontes, schemas):
    with open(os.path.join(BASE_DIR, sql_path), "r") as f:
        sql = f.read()
    schema_set = {f["schema"] for f in fontes}
    usa_schema = detecta_schema_qualificado_no_sql(sql, schema_set)
    registra_views(spark, fontes, schemas) # Removido o argumento 'usa_schema'
    sql_exec = rewrite_sql(sql, schema_set) if usa_schema else sql
    return spark.sql(sql_exec)


def exec_script(spark, script_path: str, fontes, schemas):
    """Execute a Python script under BASE_DIR and return a Spark DataFrame.

    The script is executed in an isolated namespace. It may define `df_ouro`
    or `df` as the resulting DataFrame. The function registers the configured
    fontes as temp views before running the script so the script can use them.
    """
    import runpy

    full = os.path.join(BASE_DIR, script_path)
    if not os.path.exists(full):
        raise FileNotFoundError(f"Script not found: {full}")

    # prepare views so the script can query the fontes
    registra_views(spark, fontes, schemas)

    # Execute the script in a fresh namespace and capture variables
    ns = {"__name__": "__dim_script__", "spark": spark}
    with open(full, "r", encoding="utf-8") as f:
        code = compile(f.read(), full, "exec")
        exec(code, ns)

    # Prefer explicit df_ouro, else df
    if "df_ouro" in ns:
        return ns["df_ouro"]
    if "df" in ns:
        return ns["df"]
    raise Exception(f"Script {script_path} did not produce a DataFrame named 'df_ouro' or 'df'.")

# carregar o script...

def write_single_parquet(spark, df, dim_nome, camada):
    # paths
    out_dir_local = os.path.join(PARQUET_HOME, "data", camada, "dim2inst", dim_nome)
    
    # A variável 'out_file_local' estava incorreta. Corrigido para ser o nome do arquivo.
    # O arquivo final será movido para dentro deste diretório.
    out_file_local= os.path.join(out_dir_local, f"{dim_nome}.parquet")

    # Ajuste para usar o bucket e prefixo corretos da camada
    bucket = BUCKET_OURO if camada == "ouro" else BUCKET_PRATA if camada == "prata" else BUCKET_BRONZE
    prefix = OURO_PREFIX if camada == "ouro" else PRATA_PREFIX if camada == "prata" else BRONZE_PREFIX
    out_file_s3  = f"s3a://{bucket}/{prefix}/{dim_nome}/{dim_nome}.parquet"
    tmp_dir_s3   = f"s3a://{bucket}/{prefix}/_tmp_{dim_nome}"

    preferred = None

    # LOCAL
    if WRITE_LOCAL:
        # AQUI ESTÃO OS AJUSTES
        # Remove a pasta de destino completa, se existir, para evitar o erro IsADirectoryError.
        if os.path.exists(out_dir_local):
            shutil.rmtree(out_dir_local, ignore_errors=True)
        
        tmp_dir_local = os.path.join(PARQUET_HOME, "data", "prata", "dim2inst", f"_tmp_{dim_nome}")
        # Remove o diretório temporário, caso exista.
        if os.path.exists(tmp_dir_local):
            shutil.rmtree(tmp_dir_local, ignore_errors=True)
            
        df.coalesce(1).write.mode("overwrite").parquet(tmp_dir_local)
        
        part = None
        for p in glob.glob(os.path.join(tmp_dir_local, "part-*.parquet")):
            part = p; break
        
        if not part:
            raise RuntimeError("Não foi gerado part-*.parquet no tmp local.")
        
        os.makedirs(out_dir_local, exist_ok=True)
        shutil.move(part, out_file_local)
        shutil.rmtree(tmp_dir_local, ignore_errors=True)
        log_ok(f"Parquet único (LOCAL): {out_file_local}")
        preferred = out_file_local

    # S3
    if WRITE_TO_S3 and AMBIENTE != "local":
        # ... (código do S3 que parece estar OK)
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
        Path = jvm.org.apache.hadoop.fs.Path

        df.coalesce(1).write.mode("overwrite").parquet(tmp_dir_s3)
        tmp_p = Path(tmp_dir_s3); final_p = Path(out_file_s3)
        
        # Ajuste para remover o diretório final no S3, se existir
        if fs.exists(final_p.getParent()): fs.delete(final_p.getParent(), True)

        part = None
        for st in fs.listStatus(tmp_p):
            name = st.getPath().getName()
            if name.startswith("part-") and name.endswith(".parquet"):
                part = st.getPath(); break
        if part is None:
            raise RuntimeError("Não encontrei part-*.parquet no tmp S3.")
        
        # Cria o diretório de destino
        fs.mkdirs(final_p.getParent())
        ok = fs.rename(part, final_p)
        if not ok:
            jvm.org.apache.hadoop.fs.FileUtil.copy(fs, part, fs, final_p, False, hconf)
        fs.delete(tmp_p, True)
        log_ok(f"Parquet único (S3): {out_file_s3}")
        if preferred is None:
            preferred = out_file_s3

    return preferred

def load_to_postgres(parquet_path: str, destino_schema: str, destino_tabela: str, pk_cols, **kwargs):
    """
    Loader genérico: TRUNCATE + INSERT em lote a partir de Parquet.

    Blindagens:
      - Datetimes tz-aware -> America/Sao_Paulo naive (evita +3h)
      - Datetimes 'naive' marcados como UTC no JSON -> tratados como UTC -> SP naive
      - NaN/NaT -> None
      - numpy/pandas scalars -> tipos Python puros
      - strings numéricas -> **só** convertem para número se o CAST pedir.
        (Colunas de identificador como CPF/CNPJ/CEP são preservadas como TEXTO)

    kwargs aceitos (opcionais):
      - casts: dict  {COLUNA: "int|long|float|double|numeric|decimal|string|text|varchar|timestamp|datetime|date"}
      - timestamps_local: list[str]             (mantidas como estão)
      - timestamps_assume_utc: list[str]        (naive tratadas como UTC -> SP)
    """
    import os, math, re
    import numpy as np
    import pandas as pd
    from psycopg2.extras import execute_values
    from pyspark.sql import SparkSession
    from pandas.api.types import is_datetime64_any_dtype

    TZ = "America/Sao_Paulo"

    casts = kwargs.get("casts", {}) or {}
    ts_local = set(c.lower() for c in (kwargs.get("timestamps_local", []) or []))
    ts_assume_utc = set(c.lower() for c in (kwargs.get("timestamps_assume_utc", []) or []))

    # ======== Patch: política de tipos/texto ========
    # Normaliza 'casts' para comparação em UPPER->lower
    casts_upper = {str(k).strip().upper(): str(v).lower() for k, v in casts.items()}
    NUMERIC_CASTS = {"int", "integer", "bigint", "long", "float", "double", "numeric", "decimal"}
    TEXT_CASTS    = {"string", "text", "varchar"}

    # Colunas que SEMPRE devem ser texto (preservar zeros à esquerda)
    FORCE_TEXT_COLS = {"CPF", "CNPJ", "CEP", "RG", "PIS", "NIS", "IE", "IM", "MATRICULA"}
    # =================================================

    def _upper(s): return str(s).strip().upper()

    log_b(f"Iniciando carga no Postgres -> {destino_schema}.{destino_tabela}")

    # --- S3 -> local (se aplicável)
    if parquet_path.startswith("s3a://"):
        spark = SparkSession.builder.getOrCreate()
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
        Path = jvm.org.apache.hadoop.fs.Path
        src = Path(parquet_path)
        local_tmp = f"/tmp/{destino_tabela}.parquet"
        try:
            os.remove(local_tmp)
        except FileNotFoundError:
            pass
        jvm.org.apache.hadoop.fs.FileUtil.copy(
            fs, src, fs.getLocal(fs.getWorkingDirectory().toUri()), Path(local_tmp), False, hconf
        )
        parquet_path = local_tmp
        log_i(f"Parquet baixado para: {parquet_path}")

    # --- lê Parquet
    df = pd.read_parquet(parquet_path)
    df.columns = df.columns.map(_upper)

    # --- CASTs (genérico, se fornecido)
    def _apply_cast(pdf: pd.DataFrame, col: str, target: str):
        c = _upper(col)
        if c not in pdf.columns:
            log_w(f"[CAST] coluna '{col}' não encontrada; ignorando.")
            return
        t = str(target).lower()
        if t in ("int", "integer", "bigint", "long"):
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
            pdf[c] = pdf[c].where(pd.notnull(pdf[c]), None)
            pdf[c] = pdf[c].apply(lambda x: int(x) if x is not None and not (isinstance(x, float) and math.isnan(x)) else None)
        elif t in ("float", "double", "numeric", "decimal"):
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
        elif t in ("string", "text", "varchar"):
            pdf[c] = pdf[c].astype("string")
        elif t in ("timestamp", "datetime"):
            pdf[c] = pd.to_datetime(pdf[c], errors="coerce", utc=False)
        elif t == "date":
            pdf[c] = pd.to_datetime(pdf[c], errors="coerce").dt.date
        else:
            log_w(f"[CAST] tipo alvo '{target}' não reconhecido para '{col}'; ignorando.")

    for col, target in casts.items():
        _apply_cast(df, col, target)

    # --- NORMALIZAÇÃO DE TIMESTAMPS
    # 1) tz-aware -> converte para America/Sao_Paulo e remove tz (naive)
    norm_tz = []
    for c in df.columns:
        if is_datetime64_any_dtype(df[c].dtype):
            tzinfo = getattr(df[c].dtype, "tz", None)
            if tzinfo is not None:
                df[c] = df[c].dt.tz_convert(TZ).dt.tz_localize(None)
                norm_tz.append(c)
    if norm_tz:
        log_i(f"[TZ] timestamps tz-aware normalizados -> {TZ} (naive): {', '.join(norm_tz)}")

    # 2) colunas marcadas como 'assumir UTC' (mesmo se vierem naive)
    assumed = []
    for c in df.columns:
        if c.lower() in ts_assume_utc:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.tz_convert(TZ).dt.tz_localize(None)
            assumed.append(c)
    if assumed:
        log_i(f"[TZ] timestamps assumidos como UTC -> {TZ} (naive): {', '.join(assumed)}")

    # (as marcadas como local: não fazemos nada – já estão no fuso correto)

    # --- Dedup por PK (se informado e encontrado)
    pk_upper = [_upper(p) for p in (pk_cols or [])]
    if pk_upper and all(p in df.columns for p in pk_upper):
        before = len(df)
        df = df.sort_values(by=pk_upper, ascending=True).drop_duplicates(subset=pk_upper, keep="first")
        if len(df) < before:
            log_w(f"Dedup no loader: {before} -> {len(df)}.")
    elif pk_upper:
        ausentes = [p for p in pk_upper if p not in df.columns]
        log_w(f"[PK] colunas de PK ausentes no parquet: {ausentes}. Prosseguindo sem dedup.")

    # --- NaN/NaT -> None
    df = df.where(pd.notnull(df), None)

    # --- TRUNCATE + INSERT
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {destino_schema}.{destino_tabela} CASCADE")
    conn.commit()

    cols = list(df.columns)
    sql = f"INSERT INTO {destino_schema}.{destino_tabela} ({', '.join(c.lower() for c in cols)}) VALUES %s"

    # Conversor genérico: numpy/pandas -> tipos Python puros
    num_re = re.compile(r"^[+-]?\d+(?:\.\d+)?$")

    # ======== Patch: conversão controlada por coluna ========
    def _to_python_for_pg(v, force_text: bool = False):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None

        if isinstance(v, str):
            s = v.strip()
            if s.lower() in ("nat", "nan", "none", ""):
                return None
            # NÃO converter string "numérica" para número quando a coluna for texto (ou forçada)
            if (not force_text) and num_re.match(s):
                try:
                    f = float(s)
                    return int(f) if f.is_integer() else f
                except Exception:
                    pass
            return s

        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return float(v)
        if isinstance(v, np.bool_):
            return bool(v)
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        if isinstance(v, pd.Timedelta):
            return v.to_pytimedelta()
        return v

    def rows_iter(pdf: pd.DataFrame):
        cols_upper = [c.upper() for c in pdf.columns]
        # Decide, coluna a coluna, se devemos FORÇAR TEXTO (preservar zeros à esquerda)
        as_text = []
        for c in cols_upper:
            if c in FORCE_TEXT_COLS:
                as_text.append(True)
                continue
            if c in casts_upper and casts_upper[c] in TEXT_CASTS:
                as_text.append(True)
                continue
            if c in casts_upper and casts_upper[c] in NUMERIC_CASTS:
                as_text.append(False)
                continue
            # Inferência: se dtype parecer numérico, pode converter; senão, preserve texto
            dtype = pdf[c].dtype
            is_num = pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype) or pd.api.types.is_bool_dtype(dtype)
            as_text.append(not is_num)

        for row in pdf.itertuples(index=False, name=None):
            yield tuple(_to_python_for_pg(v, force_text=as_text[i]) for i, v in enumerate(row))
    # =========================================================

    vals = list(rows_iter(df))
    execute_values(cur, sql, vals, page_size=5000)

    conn.commit()
    cur.close()
    conn.close()

    log_ok(f"Carga concluída: {len(df)} registros.")

def process_dim(spark, cfg, dim):
    nome = dim["nome"]
    schema_dest = dim["destino"]["schema"]
    tabela_dest = dim["destino"]["tabela"]
    id_projeto = 3

    log_b(f"Processando dimensão: {nome}")
    
    fontes = dim["fontes"]
    schemas = sorted({f["schema"] for f in fontes})

    if not trava_execucao_duplicada(id_projeto, nome):
        log_e(f"Já existe execução em andamento para {nome}! Saindo.")
        return 0

    dt_ref = datetime.now()
    registrar_inicio_carga(
        id_projeto=id_projeto,
        schema=schema_dest,
        tabela=nome,
        dt_referencia=dt_ref,
        home_script_executor=os.path.abspath(__file__)
    )

    try:
        if "script" in dim:
            df = exec_script(spark, dim["script"], fontes, schemas)
        else:
            df = exec_sql(spark, dim["sql"], fontes, schemas)
        df = df.select([F.col(c).alias(c.upper()) for c in df.columns])

        if dim.get("sql_extra"):
            _ = exec_sql(spark, dim["sql_extra"], fontes, schemas)

        for old, new in dim.get("renomeios", {}).items():
            if old in df.columns and new != old:
                df = df.withColumnRenamed(old, new)

        for colname, value in dim.get("defaults", {}).items():
            if colname not in df.columns:
                df = df.withColumn(colname, F.lit(value))
            else:
                df = df.withColumn(colname, F.when(F.col(colname).isNull(), F.lit(value)).otherwise(F.col(colname)))

        for colname, spark_type in dim.get("casts", {}).items():
            if colname in df.columns:
                df = df.withColumn(colname, F.col(colname).cast(spark_type))

        for ts_col in [c for c in df.columns if c.upper().startswith("DATA_")]:
            try:
                df = apply_tz_conversion(df, cfg)
                #df = df.withColumn(ts_col, from_utc_timestamp(col(ts_col), "America/Sao_Paulo"))
            except Exception:
                pass

        pk = dim["pk"]
        dup = df.groupBy(*pk).count().filter(F.col("count") > 1)
        dup_count = dup.count()
        if dup_count > 0:
            log_w(f"[{nome}] Duplicatas por {pk}: {dup_count} — salvando diagnóstico")
            diag_dir = os.path.join(PARQUET_HOME, "data", "prata", "dim2inst", nome, "_diag_duplicatas")
            (df.alias("d")
                .join(dup.select(*pk).distinct().alias("k"), pk, "inner")
                .coalesce(1).write.mode("overwrite").parquet(diag_dir))
            log_w(f"Diagnóstico: {diag_dir}")
            if STRICT_PK:
                raise Exception(f"[{nome}] STRICT_PK: duplicatas detectadas.")

        order_specs = []
        for spec in dim.get("ordenacao_pk", []):
            parts = spec.split(":")
            coln = parts[0]; mode = parts[1] if len(parts) > 1 else "desc"
            if coln in df.columns:
                if mode == "desc_nulls_last":
                    order_specs.append(F.col(coln).desc_nulls_last())
                elif mode == "asc_nulls_last":
                    order_specs.append(F.col(coln).asc_nulls_last())
                elif mode == "asc":
                    order_specs.append(F.col(coln).asc())
                else:
                    order_specs.append(F.col(coln).desc())
        w = Window.partitionBy(*pk).orderBy(*order_specs) if order_specs else Window.partitionBy(*pk).orderBy(F.lit(1))
        df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).drop("rn")

        left = df.groupBy(*pk).count().filter(F.col("count") > 1).count()
        if left > 0:
            raise Exception(f"[{nome}] Dedup falhou: ainda há duplicatas.")

        camada = dim["camada"]
        parquet_path = write_single_parquet(spark, df, nome, camada)

        if LOAD_TO_DB and dim.get("destino"):
            load_to_postgres(
                parquet_path,
                schema_dest,
                tabela_dest,
                pk,
                casts=dim.get("casts", {}),
                timestamps_local=dim.get("timestamps_local", []),
                timestamps_assume_utc=dim.get("timestamps_assume_utc", [])
            )

        qt = df.count()
        registrar_fim_carga(
            id_projeto=id_projeto,
            schema=schema_dest,
            tabela=nome,
            dt_referencia=dt_ref,
            dt_fim_carga=datetime.now(),
            qt_registros=qt,
            status="SUCESSO",
            tempo_execucao=str(datetime.now() - dt_ref),
            mensagem_erro=None
        )
        log_ok(f"[{nome}] concluída.")
        return qt

    except Exception as e:
        registrar_fim_carga(
            id_projeto=id_projeto,
            schema=schema_dest,
            tabela=nome,
            dt_referencia=dt_ref,
            dt_fim_carga=datetime.now(),
            qt_registros=0,
            status="ERRO",
            tempo_execucao=str(datetime.now() - dt_ref),
            mensagem_erro=str(e)
        )
        log_e(f"[{nome}] erro: {e}")
        raise

def main(selected_dims=None):
    inicio = datetime.now()
    log_b(f"Processo Dimensões EJUD - Ambiente: {AMBIENTE.upper()}")

    cfg = load_config()

    todas = [d["nome"] for d in cfg["dimensoes"]]
    if selected_dims:
        selected_dims = [d.strip() for d in selected_dims if d.strip()]
        inexistentes = [d for d in selected_dims if d not in todas]
        if inexistentes:
            raise ValueError(f"Dimensões não encontradas no JSON: {inexistentes}. Disponíveis: {todas}")
        dims_plan = [d for d in cfg["dimensoes"] if d["nome"] in selected_dims]
        log_i(f"Executando apenas: {', '.join([d['nome'] for d in dims_plan])}")
    else:
        dims_plan = cfg["dimensoes"]
        log_i(f"Executando todas as dimensões: {', '.join(todas)}")

    spark = spark_session()

    total_regs = 0
    for dim in dims_plan:
        total_regs += process_dim(spark, cfg, dim)

    log_ok(f"Todas as dimensões concluídas em {datetime.now() - inicio}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executor de dimensões EJUD")
    parser.add_argument(
        "--dims",
        help="Lista separada por vírgulas com os nomes das dimensões a executar (ex.: dim_magistrado_ejud,dim_classe_ejud). Se omitido, executa todas.",
        default=None
    )
    args = parser.parse_args()
    selected = [d.strip() for d in args.dims.split(",")] if args.dims else None
    main(selected_dims=selected)



