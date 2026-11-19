# -*- coding: utf-8 -*-
"""
Iceberg -> Oracle (APEX) bridge via PySpark

Objetivo:
- Ler uma tabela Iceberg no data lake
- Truncar a tabela de destino no Oracle (se existir) usando OJDBC via JVM (sem libs Python extras)
- Gravar os dados no Oracle via JDBC (append)

Observações:
- O ojdbc8.jar já deve existir no cluster (ex.: /usr/lib/spark/jars/ojdbc8.jar)
- O Secret no AWS Secrets Manager deve retornar:
  {
    "jdbc_url": "...",
    "username": "...",
    "password": "..."
  }
"""

import json
import argparse
import logging
from typing import Dict

import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# =============================================================================
# Logging
# =============================================================================
APP_NAME = "iceberg_to_oracle_apex"


def setup_logger(level: str = "INFO") -> logging.Logger:
    """
    Configura logging padrão (stdout) para o script.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    return logging.getLogger(APP_NAME)


logger = setup_logger()


# =============================================================================
# Secrets Manager
# =============================================================================
def get_secret(secret_name: str, region: str) -> Dict[str, str]:
    """
    Lê um Secret no AWS Secrets Manager e retorna um dict.

    Esperado:
      - jdbc_url
      - username
      - password
    """
    logger.info("Lendo secret no Secrets Manager: %s (region=%s)", secret_name, region)
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)

    secret_str = resp.get("SecretString")
    if not secret_str:
        # Caso binário (raro)
        secret_str = resp["SecretBinary"].decode("utf-8")

    secret = json.loads(secret_str)

    # validação mínima
    required = ["jdbc_url", "username", "password"]
    missing = [k for k in required if k not in secret or not secret.get(k)]
    if missing:
        raise ValueError(f"Secret inválido. Campos ausentes/vazios: {missing}")

    return secret


# =============================================================================
# Spark
# =============================================================================
def create_spark(app_name: str = APP_NAME) -> SparkSession:
    """
    Cria/obtém SparkSession.
    """
    logger.info("Inicializando SparkSession...")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    logger.info("SparkSession pronta.")
    return spark


# =============================================================================
# Oracle Utils (via JVM)
# =============================================================================
def oracle_truncate_if_exists(
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    oracle_table: str
) -> bool:
    """
    Conecta no Oracle via JVM (ojdbc) e:
      - verifica se a tabela existe
      - se existir, TRUNCATE TABLE

    Retorna:
      True  -> truncou
      False -> não truncou (tabela não existe)
    """
    logger.info("Verificando existência da tabela no Oracle para truncar: %s", oracle_table)

    jvm = spark._sc._gateway.jvm

    # garante driver carregado
    jvm.java.lang.Class.forName("oracle.jdbc.OracleDriver")

    conn = stmt = rs = None
    try:
        conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()

        # oracle_table pode ser "SCHEMA.TABELA" ou "TABELA"
        if "." in oracle_table:
            schema, table = oracle_table.split(".", 1)
            schema = schema.upper()
            table = table.upper()
            exists_sql = (
                "SELECT COUNT(1) AS CNT "
                "FROM ALL_TABLES "
                f"WHERE OWNER = '{schema}' AND TABLE_NAME = '{table}'"
            )
        else:
            table = oracle_table.upper()
            exists_sql = (
                "SELECT COUNT(1) AS CNT "
                "FROM USER_TABLES "
                f"WHERE TABLE_NAME = '{table}'"
            )

        rs = stmt.executeQuery(exists_sql)
        rs.next()
        exists = rs.getInt("CNT") > 0

        if not exists:
            logger.info("Tabela não existe no Oracle. Não será feito TRUNCATE: %s", oracle_table)
            return False

        logger.info("Tabela existe. Executando TRUNCATE: %s", oracle_table)
        stmt.executeUpdate(f"TRUNCATE TABLE {oracle_table}")
        logger.info("TRUNCATE concluído: %s", oracle_table)
        return True

    finally:
        # fecha recursos na ordem correta
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


# =============================================================================
# ETL
# =============================================================================
def read_iceberg_table(spark: SparkSession, iceberg_table: str) -> DataFrame:
    """
    Lê uma tabela Iceberg registrada no catálogo.
    Ex.: dw_apex.tb_localizador_processo_1inst
    """
    logger.info("Lendo tabela Iceberg: %s", iceberg_table)
    df = spark.table(iceberg_table)
    logger.info("Leitura Iceberg OK. Colunas=%d", len(df.columns))
    return df


def read_oracle_table(
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    oracle_table: str
) -> DataFrame:
    """
    Lê uma tabela do Oracle via JDBC.
    """
    logger.info("Lendo tabela Oracle para deduplicacao: %s", oracle_table)
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", oracle_table)
        .option("user", user)
        .option("password", password)
        .option("driver", "oracle.jdbc.OracleDriver")
        .load()
    )


def filtrar_apenas_novos(df_src: DataFrame, df_tgt: DataFrame) -> DataFrame:
    """
    Retorna apenas linhas de df_src que ainda não existem em df_tgt
    considerando todas as colunas em comum (null-safe).
    """
    src_by_lower = {c.lower(): c for c in df_src.columns}
    tgt_by_lower = {c.lower(): c for c in df_tgt.columns}
    common = sorted(set(src_by_lower.keys()) & set(tgt_by_lower.keys()))
    if not common:
        logger.warning("Sem colunas em comum para deduplicar. Mantendo dataset completo.")
        return df_src

    pares = [f"{src_by_lower[c]}={tgt_by_lower[c]}" for c in common]
    logger.info("Deduplicando append pelas colunas: %s", ", ".join(pares))
    tgt_distinct = df_tgt.select(*[F.col(f"`{tgt_by_lower[c]}`") for c in common]).dropDuplicates()

    cond = None
    for c in common:
        src_c = src_by_lower[c]
        tgt_c = tgt_by_lower[c]
        expr = F.col(f"src.`{src_c}`").eqNullSafe(F.col(f"tgt.`{tgt_c}`"))
        cond = expr if cond is None else (cond & expr)

    return (
        df_src.alias("src")
        .join(tgt_distinct.alias("tgt"), cond, "left_anti")
        .select("src.*")
    )


def write_oracle_append(
    df: DataFrame,
    jdbc_url: str,
    user: str,
    password: str,
    oracle_table: str,
    num_partitions: int,
    batchsize: int
) -> None:
    """
    Escreve DataFrame no Oracle via JDBC em modo append.
    """
    logger.info(
        "Escrevendo no Oracle (append). Tabela=%s | partitions=%d | batchsize=%d",
        oracle_table, num_partitions, batchsize
    )

    (
        df.repartition(num_partitions)
          .write
          .format("jdbc")
          .mode("append")
          .option("url", jdbc_url)
          .option("dbtable", oracle_table)
          .option("user", user)
          .option("password", password)
          .option("driver", "oracle.jdbc.OracleDriver")
          .option("batchsize", str(batchsize))
          .save()
    )

    logger.info("Escrita Oracle concluída: %s", oracle_table)


# =============================================================================
# Args / Main
# =============================================================================
def parse_args() -> argparse.Namespace:
    """
    Lê argumentos de linha de comando.
    """
    parser = argparse.ArgumentParser(description="Iceberg -> Oracle via PySpark + Secrets Manager")

    parser.add_argument("--iceberg-table", required=True, help="Ex: prata.tb_sua_tabela")
    parser.add_argument("--oracle-table", required=True, help="Ex: APEX.TB_DESTINO")

    parser.add_argument("--secret-name", required=True, help="Nome/ARN do secret no Secrets Manager")
    parser.add_argument("--region", default="sa-east-1", help="Região do Secrets Manager (default: sa-east-1)")

    parser.add_argument("--truncate-first", type=int, default=1, help="1=trunca se existir, 0=não trunca")
    parser.add_argument(
        "--append-only-new",
        type=int,
        default=0,
        help="1=quando truncate-first=0, tenta inserir apenas linhas novas no Oracle; 0=append direto.",
    )
    parser.add_argument("--num-partitions", type=int, default=8, help="Paralelismo na escrita JDBC")
    parser.add_argument("--batchsize", type=int, default=10000, help="Batch JDBC")

    parser.add_argument("--log-level", default="INFO", help="INFO, DEBUG, WARNING...")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Reconfigura logger conforme argumento
    global logger
    logger = setup_logger(args.log_level)

    logger.info("==== Início %s ====", APP_NAME)
    logger.info(
        "Params: iceberg=%s | oracle=%s | truncate=%s | append_only_new=%s",
        args.iceberg_table,
        args.oracle_table,
        args.truncate_first,
        args.append_only_new,
    )

    spark = create_spark()

    try:
        secret = get_secret(args.secret_name, args.region)
        jdbc_url = secret["jdbc_url"]
        username = secret["username"]
        password = secret["password"]

        # 1) TRUNCATE se existir
        if args.truncate_first == 1:
            oracle_truncate_if_exists(
                spark=spark,
                jdbc_url=jdbc_url,
                user=username,
                password=password,
                oracle_table=args.oracle_table
            )

        # 2) lê iceberg
        df = read_iceberg_table(spark, args.iceberg_table)
        df_to_write = df

        if args.truncate_first == 0 and args.append_only_new == 1:
            try:
                df_oracle = read_oracle_table(
                    spark=spark,
                    jdbc_url=jdbc_url,
                    user=username,
                    password=password,
                    oracle_table=args.oracle_table
                )
                df_to_write = filtrar_apenas_novos(df_src=df, df_tgt=df_oracle)
            except Exception as e:
                logger.warning(
                    "Nao foi possivel ler/deduplicar com a tabela Oracle (%s). "
                    "Seguindo com append completo.",
                    str(e).splitlines()[0]
                )

        if df_to_write.limit(1).count() == 0:
            logger.info("Nenhum registro novo para gravar em %s.", args.oracle_table)
        else:
            write_oracle_append(
                df=df_to_write,
                jdbc_url=jdbc_url,
                user=username,
                password=password,
                oracle_table=args.oracle_table,
                num_partitions=args.num_partitions,
                batchsize=args.batchsize
            )

        logger.info("==== Concluído com sucesso ====")

    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
