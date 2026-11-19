# -*- coding: utf-8 -*-
# 2025-12-22 by Hentzy
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
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, DateType, TimestampType

script_executor = f"s3://sgdai-projetos/2instancia/dimensao/script/carregar_dimensao_2.py"

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

def log(msg: str, level: str = "INFO") -> None:
    # Mantém compatibilidade com trechos que chamam log("...")
    if level == "WARN":
        log_w(msg)
    elif level == "ERROR":
        log_e(msg)
    else:
        log_i(msg)


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


# =======================
# Controle de execução (dbcontrole.controle_execucao_carga_projeto) via JDBC (sem psycopg2)
# =======================

def now_brt() -> datetime:
    """Retorna datetime 'naive' no fuso America/Sao_Paulo (compatível com timestamp no Postgres)."""
    try:
        return datetime.now(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    except Exception:
        # fallback: horário do sistema
        return datetime.now()

def _fmt_interval(td: timedelta) -> str:
    """Formata timedelta como string HH:MM:SS para o Postgres."""
    total_seconds = int(td.total_seconds())
    # Removemos a parte dos microssegundos aqui
    hh = total_seconds // 3600
    mm = (total_seconds % 3600) // 60
    ss = (total_seconds % 60)
    return f"{hh:02d}:{mm:02d}:{ss:02d}"

def pg_execute_returning_id(
    spark: SparkSession,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    sql: str,
    params: List[Any],
) -> int:
    """Executa INSERT ... RETURNING e devolve o primeiro inteiro retornado (id_execucao).

    Implementado via JDBC no JVM (DriverManager/PreparedStatement) para evitar dependência de psycopg2 no EMR.
    """
    jvm = spark._jvm
    conn = stmt = rs = None
    try:
        # Garante carregamento do driver JDBC
        try:
            jvm.java.lang.Class.forName("org.postgresql.Driver")
        except Exception:
            pass

        conn = jvm.java.sql.DriverManager.getConnection(pg_url, pg_user, pg_password)
        conn.setAutoCommit(False)

        stmt = conn.prepareStatement(sql)
        for i, p in enumerate(params, start=1):
            if p is None:
                stmt.setObject(i, None)
            elif isinstance(p, bool):
                stmt.setBoolean(i, p)
            elif isinstance(p, int):
                stmt.setLong(i, p)
            elif isinstance(p, float):
                stmt.setDouble(i, p)
            else:
                stmt.setString(i, str(p))

        # INSERT ... RETURNING devolve ResultSet no Postgres
        rs = stmt.executeQuery()
        if rs.next():
            id_val = rs.getLong(1)
        else:
            raise RuntimeError("INSERT RETURNING não retornou id_execucao")

        conn.commit()
        return int(id_val)

    finally:
        try:
            if rs is not None:
                rs.close()
        except Exception:
            pass
        try:
            if stmt is not None:
                stmt.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

def pg_execute(
    spark: SparkSession,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    sql: str,
    params: List[Any],
) -> None:
    """Executa UPDATE/INSERT/DELETE sem retorno via JDBC no JVM."""
    jvm = spark._jvm
    conn = stmt = None
    try:
        # Garante carregamento do driver JDBC
        try:
            jvm.java.lang.Class.forName("org.postgresql.Driver")
        except Exception:
            pass

        conn = jvm.java.sql.DriverManager.getConnection(pg_url, pg_user, pg_password)
        conn.setAutoCommit(False)

        stmt = conn.prepareStatement(sql)
        for i, p in enumerate(params, start=1):
            if p is None:
                stmt.setObject(i, None)
            elif isinstance(p, bool):
                stmt.setBoolean(i, p)
            elif isinstance(p, int):
                stmt.setLong(i, p)
            elif isinstance(p, float):
                stmt.setDouble(i, p)
            else:
                stmt.setString(i, str(p))

        stmt.executeUpdate()
        conn.commit()
    finally:
        try:
            if stmt is not None:
                stmt.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

def registrar_inicio_execucao_dimensao(
    spark: SparkSession,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    id_projeto: int,
    schema_nome: str,
    tabela_nome: str,
    dt_referencia: datetime,
    home_script_executor: str = script_executor,
    status: str = "INICIADA",
    dt_carga: Optional[datetime] = None,
) -> int:
    """Insere início na tabela de controle e retorna id_execucao (tudo em BRT vindo do Python)."""
    dt_carga_date = (dt_carga or dt_referencia).date().isoformat()
    dt_ini_str = dt_referencia.strftime("%Y-%m-%d %H:%M:%S")

    sql = """
      INSERT INTO dbcontrole.controle_execucao_carga_projeto
        (id_projeto, schema_nome, tabela_nome, qt_registros, dt_carga, dt_ini_carga, dt_fim_carga,
         dt_referencia, tempo_execucao, status, home_script_executor, mensagem_erro)
      VALUES
        (?, ?, ?, NULL, ?::date, ?::timestamp, NULL, ?::timestamp, NULL, ?, ?, NULL)
      RETURNING id_execucao
    """

    params = [
        id_projeto,
        schema_nome,
        tabela_nome,
        dt_carga_date,
        dt_ini_str,   # dt_ini_carga
        dt_ini_str,   # dt_referencia
        status,
        home_script_executor
    ]

    exec_id = pg_execute_returning_id(spark, pg_url, pg_user, pg_password, sql, params)
    log_i(f"EXEC_LOG: INÍCIO registrado com id_execucao={exec_id}")
    return exec_id

def registrar_fim_execucao_dimensao(
    spark: SparkSession,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    id_execucao: int,
    qt_registros: Optional[int],
    status: str,
    dt_ini: datetime,
    dt_fim: datetime,
    mensagem_erro: Optional[str] = None,
) -> None:
    """Atualiza fim da execução na tabela de controle."""
    tempo_exec = _fmt_interval(dt_fim - dt_ini)
    sql = """
      UPDATE dbcontrole.controle_execucao_carga_projeto
         SET status = ?,
             dt_fim_carga = ?::timestamp,
             qt_registros = ?,
             tempo_execucao = ?::interval,
             mensagem_erro = ?
       WHERE id_execucao = ?
    """
    params = [
        status,
        dt_fim.strftime("%Y-%m-%d %H:%M:%S"),
        int(qt_registros) if qt_registros is not None else None,
        tempo_exec,
        mensagem_erro,
        int(id_execucao),
    ]
    pg_execute(spark, pg_url, pg_user, pg_password, sql, params)


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
    """Lê dbcontrole.config_dimensao e converte para o formato esperado pelo process_dim.

    Ajuste: mantém destino em 'destino', mas também expõe dest_catalog/dest_db/dest_table no nível raiz
    (necessário para controle de execução e logs).
    """
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

            # ✅ Campos "flat" (importantes para log/controle)
            "dest_catalog": dest_catalog,
            "dest_db": dest_db,
            "dest_table": dest_table,

            # ✅ Campo destino no formato esperado pelo process_dim
            "destino": {
                "identifier": f"{dest_catalog}.{dest_db}.{dest_table}",
                "catalog": dest_catalog,

                # mantém as duas chaves pra compatibilidade
                "schema": dest_db,
                "db": dest_db,

                "tabela": dest_table,
                "table": dest_table,
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
    Cria SparkSession de forma limpa.
    Em EMR, todas as configurações de Iceberg/Glue JÁ FORAM PASSADAS no spark-submit da DAG.
    O código Python deve apenas dar um getOrCreate().
    """
    # Em vez de setar .config() um por um, apenas iniciamos a sessão.
    # O Spark vai herdar os parâmetros --conf que enviamos na DAG.
    return SparkSession.builder.appName(app_name).getOrCreate()

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
    import argparse
    import json
    import os
    import sys
    import traceback

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

    # Seleção
    parser.add_argument("--dims", default=None, help="Lista separada por vírgula (ex: dim_tempo).")
    parser.add_argument("--instancia", default="2inst", help="Filtro da coluna instancia (default: 2inst).")
    parser.add_argument("--grupo", default=None, help="Filtro opcional por grupo.")

    # Postgres (config + log)
    parser.add_argument("--pg-url", default="jdbc:postgresql://IGNORE:5432/IGNORE", help="JDBC URL do Postgres (pode ser IGNORE quando usar secret).")
    parser.add_argument("--pg-user", default="IGNORE", help="Usuário do Postgres (pode ser IGNORE quando usar secret).")
    parser.add_argument("--pg-password", default="IGNORE", help="Senha do Postgres (pode ser IGNORE quando usar secret).")
    parser.add_argument("--pg-secret-name", default=None, help="Nome do Secret Manager para Postgres (ex: producao/rds/dw01/postgres).")

    # Controle de execução
    parser.add_argument("--id-projeto", type=int, required=True, help="ID do projeto para controle de execução.")
    parser.add_argument("--disable-exec-log", action="store_true", help="Desabilita gravação na tabela de controle de execução.")
    parser.add_argument("--exec-log-strict", action="store_true", help="Se falhar o INSERT/UPDATE no controle_execucao, falha o job.")

    # Iceberg
    parser.add_argument("--iceberg-catalog", default=os.environ.get("ICEBERG_CATALOG", "glue_catalog"), help="Catálogo Iceberg (default: glue_catalog).")
    parser.add_argument("--warehouse", default=os.environ.get("ICEBERG_WAREHOUSE", ""), help="Warehouse Iceberg (opcional; em EMR pode vir do spark-submit).")
    parser.add_argument("--app-name", default="Dimensoes", help="Nome da aplicação Spark.")

    args = parser.parse_args()

    log_i("SCRIPT_VERSION=dim_exec_log_blindado_2025-12-22")
    log_i(f"EXEC_LOG: id_projeto={args.id_projeto} strict={args.exec_log_strict} disable={args.disable_exec_log}")

    # dims selecionadas
    selected_dims = [d.strip() for d in args.dims.split(",") if d.strip()] if args.dims else None

    # Resolve Postgres via secret (uma vez)
    pg_url_res, pg_user_res, pg_password_res = args.pg_url, args.pg_user, args.pg_password
    if args.pg_secret_name:
        pg_url_res, pg_user_res, pg_password_res = maybe_override_pg_params_from_secret(
            args.pg_url, args.pg_user, args.pg_password, args.pg_secret_name
        )
        log_i(f"EXEC_LOG: Postgres via secret. url={pg_url_res} user={pg_user_res} (senha ocultada)")
    else:
        log_w("EXEC_LOG: pg-secret-name não informado; usando pg-url/pg-user/pg-password.")

    # Iceberg base
    iceberg = IcebergCfg()
    iceberg.catalog = args.iceberg_catalog
    iceberg.warehouse = args.warehouse

    # Spark
    spark = build_spark_session(iceberg, app_name=args.app_name)

    # Carregar configuração
    if args.config:
        cfg = json.loads(read_text(args.config))
    else:
        cfg = load_dims_from_postgres(
            spark=spark,
            pg_url=pg_url_res,
            pg_user=pg_user_res,
            pg_password=pg_password_res,
            instancia=args.instancia,
            grupo=args.grupo,
            selected_dims=selected_dims,
        )

    # Se existir iceberg cfg no cfg
    try:
        iceberg_from_cfg = load_iceberg_cfg(cfg)
        if args.warehouse:
            iceberg_from_cfg.warehouse = args.warehouse
        iceberg_from_cfg.catalog = iceberg.catalog or iceberg_from_cfg.catalog
        iceberg = iceberg_from_cfg
    except Exception:
        pass

    dims = cfg.get("dimensoes") or []
    if selected_dims:
        sel = set(selected_dims)
        dims = [d for d in dims if d.get("nome") in sel]

    if not dims:
        log_w("Nenhuma dimensão encontrada para executar.")
        return

    base_prefix = args.base_prefix or ""

    # Execução
    for dim in dims:
        prev = _apply_spark_conf(spark, dim.get("_spark_conf") or {})
        dt_ini = now_brt()

        # ------------------------------------------------------------
        # DESTINO BLINDADO (para controle de execução)
        # ------------------------------------------------------------
        destino = dim.get("destino") or {}

        schema_dest = (
            dim.get("dest_db")
            or dim.get("dest_schema")
            or destino.get("schema")
            or destino.get("db")
        )
        tabela_dest = (
            dim.get("dest_table")
            or dim.get("dest_table_name")
            or destino.get("tabela")
            or destino.get("table")
        )

        # identifier para count/validação
        ident_dest = destino.get("identifier")
        if not ident_dest:
            try:
                ident_dest = resolve_dest_identifier(iceberg, dim)
            except Exception:
                ident_dest = None

        log_i(f"EXEC_LOG: dimensão={dim.get('nome')} alvo={schema_dest}.{tabela_dest} ident={ident_dest}")

        # ------------------------------------------------------------
        # REGISTRA INÍCIO
        # ------------------------------------------------------------
        exec_id = None
        if not args.disable_exec_log:
            if not schema_dest or not tabela_dest:
                msg = f"EXEC_LOG: schema/tabela destino ausentes para dim '{dim.get('nome')}'. keys={list(dim.keys())}"
                log_w(msg)
                if args.exec_log_strict:
                    raise RuntimeError(msg)
            else:
                try:
                    cmd = " ".join(sys.argv)
                    log_i("EXEC_LOG: registrando INÍCIO em dbcontrole.controle_execucao_carga_projeto")
                    exec_id = registrar_inicio_execucao_dimensao(
                        spark=spark,
                        pg_url=pg_url_res,
                        pg_user=pg_user_res,
                        pg_password=pg_password_res,
                        id_projeto=int(args.id_projeto),
                        schema_nome=str(schema_dest),
                        tabela_nome=str(tabela_dest),
                        dt_referencia=dt_ini,
                        home_script_executor=script_executor,
                        status="INICIADA",
                        dt_carga=dt_ini,
                    )
                    log_ok(f"EXEC_LOG: INÍCIO OK id_execucao={exec_id}")
                except Exception as e:
                    log_w(f"EXEC_LOG: falha ao registrar INÍCIO: {e}")
                    if args.exec_log_strict:
                        raise

        try:
            # Executa dimensão
            process_dim(spark, cfg, iceberg, dim, base_prefix)

            # Conta registros (opcional)
            qt = None
            if ident_dest:
                try:
                    qt = int(spark.table(ident_dest).count())
                except Exception as e:
                    log_w(f"Não foi possível contar registros em {ident_dest}: {e}")

            dt_fim = now_brt()

            # REGISTRA FIM (SUCESSO)
            if exec_id is not None and not args.disable_exec_log:
                try:
                    registrar_fim_execucao_dimensao(
                        spark=spark,
                        pg_url=pg_url_res,
                        pg_user=pg_user_res,
                        pg_password=pg_password_res,
                        id_execucao=int(exec_id),
                        qt_registros=qt,
                        status="SUCESSO",
                        dt_ini=dt_ini,
                        dt_fim=dt_fim,
                        mensagem_erro=None,
                    )
                    log_ok(f"EXEC_LOG: FIM OK id_execucao={exec_id} status=SUCESSO qt={qt}")
                except Exception as e:
                    log_w(f"EXEC_LOG: falha ao registrar FIM (SUCESSO): {e}")
                    if args.exec_log_strict:
                        raise

        except Exception:
            dt_fim = now_brt()
            err = traceback.format_exc()

            # REGISTRA FIM (ERRO)
            if exec_id is not None and not args.disable_exec_log:
                try:
                    registrar_fim_execucao_dimensao(
                        spark=spark,
                        pg_url=pg_url_res,
                        pg_user=pg_user_res,
                        pg_password=pg_password_res,
                        id_execucao=int(exec_id),
                        qt_registros=None,
                        status="ERRO",
                        dt_ini=dt_ini,
                        dt_fim=dt_fim,
                        mensagem_erro=err[:8000],
                    )
                    log_e(f"EXEC_LOG: FIM OK id_execucao={exec_id} status=ERRO (registrado)")
                except Exception as e2:
                    log_w(f"EXEC_LOG: falha ao registrar FIM (ERRO): {e2}")
                    if args.exec_log_strict:
                        raise
            raise

        finally:
            _restore_spark_conf(spark, prev)


if __name__ == "__main__":
    main()
