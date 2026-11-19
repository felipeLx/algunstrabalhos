# -*- coding: utf-8 -*-
"""
carregar_dimensao.py (AWS/EMR + Iceberg/Glue)

Executa dimensões definidas em JSON ou em tabela (dbcontrole.config_dimensao):
- Lê JSON e SQL do S3 (ou local)
- Carrega fontes como Iceberg (Glue Catalog) e registra TempViews
- Executa a SQL da dimensão
- Aplica defaults/casts
- Dedup por PK (com regra de ordenação opcional)
- Grava no destino como tabela Iceberg

Uso (exemplo):
spark-submit s3://sgdai-projetos/2instancia/dimensao/script/carregar_dimensao.py \
  --config s3://sgdai-projetos/2instancia/dimensao/json/dimensao.json \
  --dims dim_tempo
"""
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, DateType, TimestampType

try:
    from pyspark.sql.types import TimestampNTZType  # Spark 3.3+
except Exception:
    TimestampNTZType = None  # type: ignore


# =======================
# Logs
# =======================
def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_i(msg: str) -> None:
    print(f"[{_ts()}] ℹ️  {msg}")


def log_ok(msg: str) -> None:
    print(f"[{_ts()}] ✅ {msg}")


def log_w(msg: str) -> None:
    print(f"[{_ts()}] ⚠️  {msg}")


def log_e(msg: str) -> None:
    print(f"[{_ts()}] ❌ {msg}")


# =======================
# S3 helpers
# =======================
def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://") or uri.startswith("s3a://")


def _parse_s3(uri: str) -> Tuple[str, str]:
    """Return (bucket, key). Accepts s3:// or s3a://."""
    if uri.startswith("s3a://"):
        uri = "s3://" + uri[len("s3a://") :]
    p = urlparse(uri)
    if p.scheme != "s3":
        raise ValueError(f"URI não é S3: {uri}")
    bucket = p.netloc
    key = p.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"URI S3 inválida: {uri}")
    return bucket, key


def read_text(uri: str, encoding: str = "utf-8") -> str:
    """Read text from local path or S3 URI."""
    if _is_s3(uri):
        bucket, key = _parse_s3(uri)
        s3 = boto3.client("s3")
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return obj["Body"].read().decode(encoding)
        except ClientError as e:
            raise RuntimeError(f"Falha ao ler do S3 ({uri}): {e}") from e
    else:
        with open(uri, "r", encoding=encoding) as f:
            return f.read()


def join_s3_prefix(prefix: str, rel: str) -> str:
    """Join a base s3 prefix with a relative path."""
    if _is_s3(rel) or rel.startswith("/"):
        return rel
    return prefix.rstrip("/") + "/" + rel.lstrip("/")


# =======================
# Iceberg config
# =======================
@dataclass
class IcebergCfg:
    catalog: str = "glue_catalog"
    warehouse: Optional[str] = None

    # Mapeamento padrão (bem compatível com o padrão "schema.table" -> "schema_table")
    source_db_template: str = "{layer}"          # ex: bronze / prata / ouro
    source_table_template: str = "{schema}_{table}"  # ex: ejud_feriado

    dest_db_template: str = "{layer}"            # ex: ouro
    dest_table_template: str = "{schema}_{table}"    # ex: dim2inst_dim_tempo

    default_source_layer: str = "bronze"

    # Propriedades (opcionais) aplicadas na criação/substituição
    table_properties: Dict[str, Any] = None  # type: ignore



# =======================
# Config via Postgres (dbcontrole.config_dimensao) + Secrets Manager
# =======================

def get_secret_dict(secret_name: str) -> Dict[str, str]:
    """Lê Secret no AWS Secrets Manager e retorna como dict."""
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "sa-east-1"
    )
    log_i(f"Usando região '{region}' para acessar o Secrets Manager (secret: {secret_name})")
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def maybe_override_pg_params_from_secret(
    pg_url_cli: str,
    pg_user_cli: str,
    pg_password_cli: str,
    pg_secret_name: Optional[str],
) -> Tuple[str, str, str]:
    """Mantém compatibilidade com seus submits: IGNORE via CLI e secret manda."""
    if not pg_secret_name:
        log_i("Nenhum secret de Postgres informado, usando parâmetros passados via linha de comando.")
        return pg_url_cli, pg_user_cli, pg_password_cli

    log_i(f"Carregando parâmetros de Postgres do Secret {pg_secret_name}...")
    s = get_secret_dict(pg_secret_name)

    host = s.get("host")
    port = s.get("port", "5432")
    dbname = s.get("dbname") or s.get("database")
    user = s.get("username") or s.get("user")
    password = s.get("password")

    if not (host and dbname and user and password):
        raise RuntimeError(
            f"Secret {pg_secret_name} não possui todos os campos necessários "
            f"(host, dbname, username, password). Conteúdo: {list(s.keys())}"
        )

    url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    log_i(f"Config de Postgres sobrescrita a partir do secret {pg_secret_name}: url='{url}', user='{user}' (senha ocultada).")
    return url, user, password


def read_postgres_table(
    spark: SparkSession,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    sql: str,
) -> DataFrame:
    dbtable = f"({sql}) AS cfg"
    log_i(f"Lendo configuração no Postgres com JDBC: url='{pg_url}', user='{pg_user}'")

    df = (
        spark.read.format("jdbc")
        .option("url", pg_url)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", dbtable)
        .load()
    )
    # df.count() dispara job - ok porque config é pequena
    log_i(f"Configuração lida do Postgres: {df.count()} linha(s).")
    return df


def _ensure_json(v: Any, default: Any) -> Any:
    """Converte jsonb/string->obj python. Tenta lidar com JSON duplamente serializado."""
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return default
        try:
            return json.loads(s)
        except Exception:
            try:
                return json.loads(json.loads(s))
            except Exception:
                return default
    return default


def _normalize_sources(sources: Any) -> List[Dict[str, Any]]:
    srcs = _ensure_json(sources, [])
    if not isinstance(srcs, list):
        return []
    out: List[Dict[str, Any]] = []
    for s in srcs:
        if not isinstance(s, dict):
            continue
        catalog = s.get("catalog", "glue_catalog")
        db = s.get("db") or s.get("schema")
        table = s.get("table") or s.get("tabela")
        view = s.get("view") or s.get("alias")
        opcional = bool(s.get("opcional", s.get("optional", False)))

        if not (db and table):
            continue

        out.append(
            {
                "identifier": f"{catalog}.{db}.{table}",
                "schema": str(db),
                "tabela": str(table),
                "view": str(view) if view else None,
                "opcional": opcional,
                # layer é apenas informativo aqui; se quiser controlar, adicione no JSON de sources
                "layer": s.get("layer") or "bronze",
            }
        )
    return out


def load_dims_from_postgres(
    spark: SparkSession,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    instancia: str,
    grupo: Optional[str],
    selected_dims: Optional[List[str]],
) -> Dict[str, Any]:
    """Lê dbcontrole.config_dimensao e converte para o formato esperado pelo process_dim."""
    filtro_dims = ""
    if selected_dims:
        dims_sql = ",".join([f"'{d.strip()}'" for d in selected_dims if d.strip()])
        if dims_sql:
            filtro_dims = f" AND dim_nome IN ({dims_sql})"

    filtro_grupo = ""
    if grupo:
        filtro_grupo = f" AND grupo = '{grupo}'"

    sql_cfg = f"""
    SELECT
      dim_nome, ordem_execucao, instancia, grupo, descricao,
      sql_s3_path,
      dest_catalog, dest_db, dest_table,
      write_mode, pk_cols,
      sources, spark_conf, rules, tags
    FROM dbcontrole.config_dimensao
    WHERE ativo = true
      AND (instancia = '{instancia}' OR instancia IS NULL)
      {filtro_grupo}
      {filtro_dims}
    ORDER BY ordem_execucao, dim_nome
    """

    df = read_postgres_table(spark, pg_url, pg_user, pg_password, sql_cfg)
    rows = [r.asDict(recursive=True) for r in df.collect()]

    dims: List[Dict[str, Any]] = []
    for r in rows:
        nome = r.get("dim_nome")
        if not nome:
            continue

        dest_catalog = r.get("dest_catalog") or "glue_catalog"
        dest_db = r.get("dest_db")
        dest_table = r.get("dest_table")
        if not (dest_db and dest_table):
            raise ValueError(f"Config inválida (dest_db/dest_table) para dimensão '{nome}': {r}")

        dim: Dict[str, Any] = {
            "nome": nome,
            "sql": r.get("sql_s3_path"),
            "fontes": _normalize_sources(r.get("sources")),
            "pk": list(r.get("pk_cols") or []),
            "write_mode": r.get("write_mode") or "overwrite",
            "destino": {
                "identifier": f"{dest_catalog}.{dest_db}.{dest_table}",
                "schema": dest_db,
                "tabela": dest_table,
            },
            # metadados úteis
            "descricao": r.get("descricao"),
            "instancia": r.get("instancia"),
            "grupo": r.get("grupo"),
            "_spark_conf": _ensure_json(r.get("spark_conf"), {}),
            "_rules": _ensure_json(r.get("rules"), {}),
            "_tags": r.get("tags"),
        }

        # Merge de regras (opcional) para habilitar defaults/casts/ordenacao_pk/etc via coluna rules
        rules = dim.get("_rules") or {}
        if isinstance(rules, dict):
            # não sobrescrever campos principais
            for k, v in rules.items():
                if k in ("nome", "sql", "fontes", "destino"):
                    continue
                dim.setdefault(k, v)

        if not dim.get("sql"):
            raise ValueError(f"Dimensão '{nome}' sem sql_s3_path.")

        dims.append(dim)

    return {"dimensoes": dims}
def load_iceberg_cfg(cfg: Dict[str, Any]) -> IcebergCfg:
    icfg = (cfg.get("iceberg") or {}) if isinstance(cfg, dict) else {}
    out = IcebergCfg()
    out.catalog = icfg.get("catalog", os.environ.get("ICEBERG_CATALOG", out.catalog))
    out.warehouse = icfg.get("warehouse", os.environ.get("ICEBERG_WAREHOUSE", out.warehouse))

    out.source_db_template = icfg.get("source_db_template", out.source_db_template)
    out.source_table_template = icfg.get("source_table_template", out.source_table_template)
    out.dest_db_template = icfg.get("dest_db_template", out.dest_db_template)
    out.dest_table_template = icfg.get("dest_table_template", out.dest_table_template)
    out.default_source_layer = icfg.get("default_source_layer", out.default_source_layer)
    out.table_properties = icfg.get("table_properties", {}) or {}
    return out


def build_spark_session(iceberg: IcebergCfg, app_name: str = "Dimensoes") -> SparkSession:
    """
    Cria SparkSession.
    - Em EMR, os configs de Iceberg/Glue normalmente vêm do spark-submit/spark-defaults.
    - Se 'iceberg.warehouse' estiver definido, aplica também no builder (não é obrigatório).
    """
    b = SparkSession.builder.appName(app_name)

    # Configurar Iceberg/Glue no builder é útil em modo local/teste. Em EMR, pode vir do submit.
    b = b.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    b = b.config(f"spark.sql.catalog.{iceberg.catalog}", "org.apache.iceberg.spark.SparkCatalog")
    b = b.config(f"spark.sql.catalog.{iceberg.catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    b = b.config(f"spark.sql.catalog.{iceberg.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

    if iceberg.warehouse:
        b = b.config(f"spark.sql.catalog.{iceberg.catalog}.warehouse", iceberg.warehouse)

    b = (
        b.config("spark.sql.session.timeZone", os.environ.get("SPARK_TZ", "America/Sao_Paulo"))
         .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
         .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    )
    return b.getOrCreate()
def iceberg_ident(catalog: str, db: str, table: str) -> str:
    return f"{catalog}.{db}.{table}"


def ensure_database(spark: SparkSession, catalog: str, db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")


# =======================
# SQL rewriting (compatível com o seu padrão antigo)
# =======================
def detecta_schema_qualificado_no_sql(sql_text: str, schemas: set) -> bool:
    low = sql_text.lower()
    return any((s.lower() + ".") in low for s in schemas)


def rewrite_sql(sql_text: str, schemas: set, catalogs: set) -> str:
    """
    Reescreve referências de fontes para bater com TempViews criadas pelo script.

    Regras:
      1) catalog.schema.tabela  -> schema_tabela
      2) catalog.schema_tabela  -> schema_tabela   (caso o SQL já venha "meio reescrito")
      3) schema.tabela          -> schema_tabela   (apenas quando NÃO for 3-part)

    Obs: mantemos a lógica "best-effort" e case-insensitive.
    """
    out = sql_text

    # 2) Se já tiver "catalog.schema_" (ex: glue_catalog.ejud_feriado), remove o prefixo do catálogo
    for cat in sorted({c for c in catalogs if c}, key=len, reverse=True):
        for sch in sorted({s for s in schemas if s}, key=len, reverse=True):
            out = re.sub(
                rf"(?i)\b`?{re.escape(cat)}`?\s*\.\s*`?{re.escape(sch)}`?_",
                f"{sch}_",
                out,
            )

    # 1) catalog.schema.tabela -> schema_tabela
    for cat in sorted({c for c in catalogs if c}, key=len, reverse=True):
        for sch in sorted({s for s in schemas if s}, key=len, reverse=True):
            out = re.sub(
                rf"(?i)\b`?{re.escape(cat)}`?\s*\.\s*`?{re.escape(sch)}`?\s*\.\s*`?([A-Za-z_][\w]*)`?\b",
                lambda m: f"{sch}_{m.group(1)}",
                out,
            )

    # 3) schema.tabela -> schema_tabela (somente se NÃO estiver precedido por '.')
    for sch in sorted({s for s in schemas if s}, key=len, reverse=True):
        out = re.sub(
            rf"(?i)(?<!\.)\b`?{re.escape(sch)}`?\s*\.\s*`?([A-Za-z_][\w]*)`?\b",
            lambda m: f"{sch}_{m.group(1)}",
            out,
        )

    return out




# =======================
# Fonte -> TempView (Iceberg)
# =======================
def _infer_layer_and_schema(path_schema: str, default_layer: str) -> Tuple[str, str]:
    """
    Aceita:
      - "prata/ejud" ou "ouro/ejud" (mantém compatibilidade)
      - "ejud" (assume default_layer)
    Retorna: (layer, schema_name)
    """
    if path_schema.startswith("prata/"):
        return "prata", path_schema.split("/", 1)[1]
    if path_schema.startswith("ouro/"):
        return "ouro", path_schema.split("/", 1)[1]
    if path_schema.startswith("bronze/"):
        return "bronze", path_schema.split("/", 1)[1]
    return default_layer, path_schema


def resolve_source_identifier(iceberg: IcebergCfg, fonte: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Retorna:
      (identifier, layer, schema_name, table_name)
    """
    # caminho fully qualified opcional
    ident = fonte.get("identifier") or fonte.get("iceberg_identifier")
    if ident:
        # tentar extrair db/table (best-effort)
        return ident, fonte.get("layer", iceberg.default_source_layer), fonte.get("schema", ""), fonte.get("tabela", "")

    layer = fonte.get("layer")
    schema_field = fonte.get("schema", "")
    table = fonte.get("tabela") or fonte.get("table")
    if not table:
        raise ValueError(f"Fonte sem 'tabela': {fonte}")

    if not layer:
        layer, schema_name = _infer_layer_and_schema(schema_field, iceberg.default_source_layer)
    else:
        _, schema_name = _infer_layer_and_schema(schema_field, iceberg.default_source_layer)

    db = iceberg.source_db_template.format(layer=layer, schema=schema_name, table=table)
    tbl = iceberg.source_table_template.format(layer=layer, schema=schema_name, table=table)
    return iceberg_ident(iceberg.catalog, db, tbl), layer, schema_name, table


def create_empty_view(spark: SparkSession, view_name: str, empty_schema: StructType) -> None:
    df_empty = spark.createDataFrame([], schema=empty_schema)
    df_empty.createOrReplaceTempView(view_name)


def registra_views_iceberg(spark: SparkSession, iceberg: IcebergCfg, fontes: List[Dict[str, Any]]) -> None:
    """
    Para cada fonte:
      - carrega Iceberg table
      - cria views compatíveis:
        1) tabela
        2) schema_tabela
        3) alias (se houver)
        4) schema_alias (se houver)
    """
    for item in fontes:
        alias = item.get("view") or item.get("alias")
        ident, layer, schema_name, tabela = resolve_source_identifier(iceberg, item)
        opt = bool(item.get("opcional", False))

        log_i(f"Fonte -> {ident} (layer={layer}, schema={schema_name}, tabela={tabela})")

        try:
            df = spark.table(ident)
        except AnalysisException as e:
            # Fallback: se for opcional, cria view vazia
            if opt:
                log_w(f"Fonte não encontrada (opcional). Criando view vazia para '{tabela}'. Detalhe: {e}")
                empty_schema = item.get("empty_schema")
                if empty_schema:
                    # usuário pode fornecer lista: [{"name":"dtfer","type":"date"}]
                    fields = []
                    for f in empty_schema:
                        t = (f.get("type") or "").lower()
                        if t == "date":
                            fields.append(StructField(f["name"], DateType(), True))
                        else:
                            # default -> string
                            from pyspark.sql.types import StringType
                            fields.append(StructField(f["name"], StringType(), True))
                    st = StructType(fields)
                else:
                    # caso especial: feriado (para a dim_tempo)
                    st = StructType([StructField("dtfer", DateType(), True)])
                create_empty_view(spark, tabela, st)
                create_empty_view(spark, f"{schema_name}_{tabela}", st)
                if alias and alias != tabela:
                    create_empty_view(spark, alias, st)
                    create_empty_view(spark, f"{schema_name}_{alias}", st)
                continue
            raise

        # Views
        df.createOrReplaceTempView(tabela)
        df.createOrReplaceTempView(f"{schema_name}_{tabela}")

        if alias and alias != tabela:
            df.createOrReplaceTempView(alias)
            df.createOrReplaceTempView(f"{schema_name}_{alias}")


def exec_sql(spark: SparkSession, sql_uri: str, iceberg: IcebergCfg, fontes: List[Dict[str, Any]]) -> DataFrame:
    sql_text = read_text(sql_uri)
    schema_set = {f.get("schema") for f in fontes if f.get("schema")}
    schema_set = {s.split("/", 1)[1] if isinstance(s, str) and "/" in s else s for s in schema_set}
    schema_set = {s for s in schema_set if isinstance(s, str) and s}

    usa_schema = detecta_schema_qualificado_no_sql(sql_text, schema_set) if schema_set else False

    registra_views_iceberg(spark, iceberg, fontes)

    catalogs = {str(f.get('identifier')).split('.', 1)[0] for f in fontes if f.get('identifier')}
    sql_exec = rewrite_sql(sql_text, schema_set, catalogs) if usa_schema else sql_text
    return spark.sql(sql_exec)


# =======================
# Transformações comuns
# =======================
def apply_defaults_and_casts(df: DataFrame, dim_cfg: Dict[str, Any]) -> DataFrame:
    # defaults
    for colname, value in (dim_cfg.get("defaults") or {}).items():
        if colname in df.columns:
            df = df.withColumn(colname, F.when(F.col(colname).isNull(), F.lit(value)).otherwise(F.col(colname)))

    # casts
    for colname, spark_type in (dim_cfg.get("casts") or {}).items():
        if colname in df.columns:
            df = df.withColumn(colname, F.col(colname).cast(spark_type))

    return df


def apply_tz_conversion(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """
    Converte timestamps (UTC -> America/Sao_Paulo) por padrão,
    exceto os listados em cfg.timestamps_local.
    """
    local_cfg = set(c.lower() for c in (cfg.get("timestamps_local") or []))
    assume_utc = set(c.lower() for c in (cfg.get("timestamps_assume_utc") or []))

    for name, dtype in df.dtypes:
        # df.dtypes dá string types; precisamos do schema real para checar TimestampNTZ
        pass

    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        is_ts = isinstance(dtype, TimestampType) or (TimestampNTZType and isinstance(dtype, TimestampNTZType))
        if not is_ts:
            continue

        if name.lower() in local_cfg:
            continue

        # TimestampNTZ: sem fuso; só converte se explicitamente marcado como UTC
        if TimestampNTZType and isinstance(dtype, TimestampNTZType) and name.lower() not in assume_utc:
            continue

        df = df.withColumn(name, F.from_utc_timestamp(F.col(name), "America/Sao_Paulo"))

    return df



def resolve_columns_case_insensitive(df: DataFrame, cols: List[str]) -> List[str]:
    """Resolve nomes de colunas (case-insensitive) para os nomes reais do DataFrame.
    Lança erro amigável se alguma coluna não existir.
    """
    if not cols:
        return []
    col_map = {c.lower(): c for c in df.columns}
    resolved: List[str] = []
    missing: List[str] = []
    for c in cols:
        if c in df.columns:
            resolved.append(c)
        else:
            rc = col_map.get(str(c).lower())
            if rc:
                resolved.append(rc)
            else:
                missing.append(c)
    if missing:
        raise ValueError(
            "PK configurada não bate com as colunas retornadas pela SQL. "
            f"PK (config)={cols} | faltando={missing} | colunas_df={df.columns}"
        )
    return resolved

def dedup_by_pk(df: DataFrame, pk: List[str], order_specs: List[Any]) -> DataFrame:
    if not pk:
        return df

    w = Window.partitionBy(*pk).orderBy(*order_specs) if order_specs else Window.partitionBy(*pk).orderBy(F.lit(1))
    df2 = df.withColumn("__rn", F.row_number().over(w)).filter(F.col("__rn") == 1).drop("__rn")

    # sanity-check
    left = df2.groupBy(*pk).count().filter(F.col("count") > 1).count()
    if left > 0:
        raise RuntimeError(f"Dedup falhou: ainda há {left} chaves duplicadas.")
    return df2


# =======================
# Write Iceberg
# =======================
def resolve_dest_identifier(iceberg: IcebergCfg, dim_cfg: Dict[str, Any]) -> Tuple[str, str, str]:
    camada = dim_cfg.get("camada", "ouro")
    destino = dim_cfg.get("destino") or {}
    schema_dest = destino.get("schema") or destino.get("database") or destino.get("db")
    tabela_dest = destino.get("tabela") or destino.get("table")

    if destino.get("identifier") or destino.get("iceberg_identifier"):
        ident = destino.get("identifier") or destino.get("iceberg_identifier")
        # best-effort: db/table não usados aqui
        return ident, camada, str(schema_dest or "")

    if not schema_dest or not tabela_dest:
        raise ValueError(f"Dimensão sem destino válido: {dim_cfg.get('nome')} -> {destino}")

    db = iceberg.dest_db_template.format(layer=camada, schema=schema_dest, table=tabela_dest)
    tbl = iceberg.dest_table_template.format(layer=camada, schema=schema_dest, table=tabela_dest)
    return iceberg_ident(iceberg.catalog, db, tbl), camada, schema_dest


def write_iceberg_table(df: DataFrame, iceberg: IcebergCfg, ident: str, mode: str, props: Dict[str, Any]) -> None:
    """
    mode:
      - create_or_replace (default)
      - append
    """
    writer = df.writeTo(ident).using("iceberg")
    for k, v in (props or {}).items():
        writer = writer.tableProperty(str(k), str(v))

    m = (mode or "create_or_replace").lower()
    if m in ("overwrite", "replace", "create_or_replace", "createorreplace"):
        writer.createOrReplace()
    elif m == "append":
        # se não existir, cria; senão, append
        try:
            writer.append()
        except Exception:
            writer.create()
    else:
        raise ValueError(f"write_mode inválido: {mode}")


# =======================
# Orquestração
# =======================
def process_dim(spark: SparkSession, cfg: Dict[str, Any], iceberg: IcebergCfg, dim: Dict[str, Any], base_prefix: str) -> None:
    nome = dim["nome"]
    log_ok(f"=== Processando dimensão: {nome} ===")

    # resolve SQL
    sql_ref = dim["sql"]
    sql_uri = join_s3_prefix(base_prefix, sql_ref) if base_prefix else sql_ref

    # executar SQL
    fontes = dim.get("fontes") or []
    df = exec_sql(spark, sql_uri, iceberg, fontes)

    # defaults/casts
    df = apply_defaults_and_casts(df, dim)

    # tz conversion (se você quiser habilitar globalmente)
    if cfg.get("convert_timestamps", False):
        df = apply_tz_conversion(df, cfg)

    # dedup
    pk_cfg = dim.get("pk") or []
    # resolve PK (case-insensitive)
    pk = resolve_columns_case_insensitive(df, [str(c) for c in pk_cfg])

    order_specs: List[Any] = []
    for spec in (dim.get("ordenacao_pk") or []):
        parts = str(spec).split(":")
        coln = parts[0]
        mode = parts[1] if len(parts) > 1 else "desc"
        # resolve nome (case-insensitive); se não existir, ignora ordenação
        col_real = next((c for c in df.columns if c.lower() == str(coln).lower()), None)
        if not col_real:
            continue
            if mode == "desc_nulls_last":
                order_specs.append(F.col(col_real).desc_nulls_last())
            elif mode == "asc_nulls_last":
                order_specs.append(F.col(col_real).asc_nulls_last())
            elif mode == "asc":
                order_specs.append(F.col(col_real).asc())
            else:
                order_specs.append(F.col(col_real).desc())

    df = dedup_by_pk(df, pk, order_specs)

    # write iceberg
    ident, camada, _schema_dest = resolve_dest_identifier(iceberg, dim)
    # garante database (best-effort: extrai db do ident)
    try:
        _, db, _tbl = ident.split(".", 2)
        ensure_database(spark, iceberg.catalog, db)
    except Exception:
        pass

    write_mode = dim.get("write_mode") or (cfg.get("write_mode") or "create_or_replace")
    props = dict(iceberg.table_properties or {})
    props.update(dim.get("table_properties") or {})

    log_i(f"Gravando Iceberg: {ident} (mode={write_mode})")
    write_iceberg_table(df, iceberg, ident, write_mode, props)
    log_ok(f"Dimensão OK: {nome} -> {ident}")



def _apply_spark_conf(spark: SparkSession, conf: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Aplica spark.conf.set() e devolve valores anteriores para restauração."""
    prev: Dict[str, Optional[str]] = {}
    if not conf or not isinstance(conf, dict):
        return prev
    for k, v in conf.items():
        if v is None:
            continue
        try:
            prev[k] = spark.conf.get(str(k), None)
        except Exception:
            prev[k] = None
        spark.conf.set(str(k), str(v))
    return prev


def _restore_spark_conf(spark: SparkSession, prev: Dict[str, Optional[str]]) -> None:
    for k, v in (prev or {}).items():
        try:
            if v is None:
                # não há unset; set para vazio é ok (e não quebra jobs). Se preferir, remova.
                spark.conf.set(str(k), "")
            else:
                spark.conf.set(str(k), v)
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Executor de dimensões (AWS Iceberg/Glue)")

    # Modo 1: JSON (legado / fallback)
    parser.add_argument(
        "--config",
        default=None,
        help="URI do JSON de dimensões (s3://... ou caminho local). Se omitido, usa tabela dbcontrole.config_dimensao no Postgres.",
    )
    parser.add_argument(
        "--base-prefix",
        default=None,
        help="Prefixo base (s3://...) usado para resolver caminhos relativos (sql, etc). Ex: s3://sgdai-projetos/2instancia/dimensao",
    )

    # Seleção de dimensões
    parser.add_argument("--dims", default=None, help="Lista separada por vírgula (ex: dim_tempo).")
    parser.add_argument("--instancia", default="2inst", help="Filtro da coluna instancia (default: 2inst).")
    parser.add_argument("--grupo", default=None, help="Filtro opcional por grupo.")

    # Postgres (config table)
    parser.add_argument("--pg-url", default="jdbc:postgresql://IGNORE:5432/IGNORE", help="JDBC URL do Postgres (pode ser IGNORE quando usar secret).")
    parser.add_argument("--pg-user", default="IGNORE", help="Usuário do Postgres (pode ser IGNORE quando usar secret).")
    parser.add_argument("--pg-password", default="IGNORE", help="Senha do Postgres (pode ser IGNORE quando usar secret).")
    parser.add_argument("--pg-secret-name", default=None, help="Nome do Secret Manager para Postgres (ex: producao/rds/dw01/postgres).")

    # Iceberg
    parser.add_argument("--iceberg-catalog", default=os.environ.get("ICEBERG_CATALOG", "glue_catalog"), help="Catálogo Iceberg (default: glue_catalog).")
    parser.add_argument("--warehouse", default=os.environ.get("ICEBERG_WAREHOUSE", ""), help="Warehouse Iceberg (opcional; em EMR pode vir do spark-submit).")
    parser.add_argument("--app-name", default="Dimensoes-2inst", help="Nome da aplicação Spark.")

    args = parser.parse_args()
    selected = [d.strip() for d in args.dims.split(",") if d.strip()] if args.dims else None

    # Iceberg cfg
    iceberg = IcebergCfg()
    iceberg.catalog = args.iceberg_catalog
    iceberg.warehouse = args.warehouse

    # Spark
    spark = build_spark_session(iceberg, app_name=args.app_name)

    # Carregar config
    if args.config:
        cfg = json.loads(read_text(args.config))
    else:
        # Resolve pg params via secret (padrão das suas cargas)
        pg_url, pg_user, pg_password = maybe_override_pg_params_from_secret(
            args.pg_url, args.pg_user, args.pg_password, args.pg_secret_name
        )
        cfg = load_dims_from_postgres(
            spark=spark,
            pg_url=pg_url,
            pg_user=pg_user,
            pg_password=pg_password,
            instancia=args.instancia,
            grupo=args.grupo,
            selected_dims=selected,
        )

    # Iceberg cfg pode vir do JSON (se existir)
    try:
        iceberg_from_cfg = load_iceberg_cfg(cfg)
        # se --warehouse foi passado, preferir args
        if args.warehouse:
            iceberg_from_cfg.warehouse = args.warehouse
        iceberg_from_cfg.catalog = iceberg.catalog or iceberg_from_cfg.catalog
        iceberg = iceberg_from_cfg
    except Exception:
        # se não há bloco iceberg no cfg (config table), segue com iceberg já montado
        pass

    dims = cfg.get("dimensoes") or []
    if selected:
        sel = set(selected)
        dims = [d for d in dims if d.get("nome") in sel]

    if not dims:
        log_w("Nenhuma dimensão selecionada/encontrada para executar.")
        return

    base_prefix = args.base_prefix or ""

    for dim in dims:
        # aplica spark_conf por dimensão (opcional)
        prev = _apply_spark_conf(spark, dim.get("_spark_conf") or {})
        try:
            process_dim(spark, cfg, iceberg, dim, base_prefix)
        finally:
            _restore_spark_conf(spark, prev)


if __name__ == "__main__":
    main()
