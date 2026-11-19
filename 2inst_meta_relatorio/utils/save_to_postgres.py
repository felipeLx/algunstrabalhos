import logging
from typing import Optional, Dict, Tuple
import psycopg2
import psycopg2.extras
import os
import sys
import importlib.util
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import DataFrame
PARQUET_HOME = f"/opt/parquet"
load_dotenv(dotenv_path=os.path.join(PARQUET_HOME, "airflow", "dotenv", ".env"),  override=True)
sys.path.append(os.path.join(PARQUET_HOME, "projetos"))
sys.path.insert(0, os.path.join(PARQUET_HOME, "projetos"))
import json
logger = logging.getLogger(__name__)


def _get_pg_params(pg_config: Optional[Dict] = None) -> Dict:
    """Return psycopg2 connection params from pg_config or environment variables."""
    if pg_config:
        # allow passing psycopg2-style keys
        return {
            "host": pg_config.get("host"),
            "port": pg_config.get("port", 5432),
            "dbname": pg_config.get("dbname") or pg_config.get("database"),
            "user": pg_config.get("user"),
            "password": pg_config.get("password"),
        }

    pg_host = os.environ.get('POSTGRESQL_SL_HOST', os.environ.get('PG_HOST'))
    pg_port = os.environ.get('POSTGRESQL_SL_PORT', os.environ.get('PG_PORT', '5432'))
    pg_db = os.environ.get('POSTGRESQL_SL_DBNAME', os.environ.get('PG_DB'))
    pg_user = os.environ.get('POSTGRESQL_SL_USER', os.environ.get('PG_USER'))
    pg_pass = os.environ.get('POSTGRESQL_SL_PASSWORD', os.environ.get('PG_PASS'))

    if not all([pg_host, pg_db, pg_user, pg_pass]):
        raise RuntimeError("Missing Postgres credentials in environment (POSTGRESQL_SL_* or PG_*)")

    return {
        "host": pg_host,
        "port": int(pg_port),
        "dbname": pg_db,
        "user": pg_user,
        "password": pg_pass,
    }


def _connect_pg(pg_config: Optional[Dict] = None):
    params = _get_pg_params(pg_config)
    return psycopg2.connect(**params)

def save_df_to_postgres(df, schema: str, table_name: str, mode_user: str, pg_config: Optional[Dict] = None):
    
    # 1. Get Credentials (Env Vars)
    pg_host = os.environ.get('POSTGRESQL_SL_HOST', os.environ.get('PG_HOST'))
    pg_port = os.environ.get('POSTGRESQL_SL_PORT', os.environ.get('PG_PORT', '5432'))
    pg_db = os.environ.get('POSTGRESQL_SL_DBNAME', os.environ.get('PG_DB'))
    pg_user = os.environ.get('POSTGRESQL_SL_USER', os.environ.get('PG_USER'))
    pg_pass = os.environ.get('POSTGRESQL_SL_PASSWORD', os.environ.get('PG_PASS'))
    
    if not all([pg_host, pg_db, pg_user, pg_pass]):
        raise RuntimeError("Missing PG_* environment variables.")

    # reWriteBatchedInserts=true turns thousands of tiny inserts into one big packet.
    jdbc_url = f"jdbc:postgresql://172.20.30.141:5432/postgres?reWriteBatchedInserts=true"

    # 3. Tuning Properties
    pg_properties = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",         
        "isolationLevel": "READ_COMMITTED"
    }

    target_table = f"{schema}.{table_name}"
    
    logger.info(f"🚀 Starting Distributed Write to {target_table} (Mode: {mode_user})...")

    # 4. TRUNCATE LOGIC (Optional but recommended for 'overwrite')
    # Spark's default 'overwrite' drops the table and recreates it (destroying indexes/schemas).
    # If you want to keep the schema and just empty data, use truncate.
    write_options = df.write.mode(mode_user)
    if mode_user == "overwrite":
        write_options = write_options.option("truncate", "true")

    try:
        # 5. THE WRITE (Coalesce is Key!)
        # We coalesce to ~20 to give the DB 20 heavy streams instead of 2000 tiny ones.
        # This matches the number of CPU cores a typical Postgres server has.
        df.coalesce(20).write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", target_table) \
            .options(**pg_properties) \
            .save()
            
        logger.info(f"✅ Successfully wrote to {target_table}")
        return True

    except Exception as e:
        logger.error(f"❌ JDBC Write Failed: {e}")
        raise

def save_parquet_to_postgres(df: DataFrame, schema: str, table_name: str, mode: str, pg_config: Optional[Dict] = None):
    
    # Delegate to existing writer
    result = save_df_to_postgres_no_jar(df, schema, table_name, mode=mode)
    
    return result




def save_df_to_postgres_no_jar(df, schema, table_name, mode="append", delete_condition=None, pg_config: Optional[Dict] = None):
    
    logger.info(f"Salvando DataFrame no PostgreSQL via psycopg2: {schema}.{table_name} (mode={mode})")
    conn_driver = _connect_pg(pg_config)
    
    # Driver-side delete or truncate for overwrite mode
    if mode == "overwrite":
        try:
            conn_driver.autocommit = True
            with conn_driver.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {schema}.{table_name}")
            conn_driver.close()
            logger.info(f"Truncated {schema}.{table_name} on driver before overwrite")
        except Exception as e:
            logger.warning(f"Could not truncate table {schema}.{table_name}: {e}")
    elif delete_condition:
        try:
            import re as _re
            import io as _io
            conn_driver.autocommit = True
            # Detecta padrão: "meta_ano = N AND id_processo IN ('a','b',...)" com lista grande
            # Nesse caso usa COPY + temp table em vez de IN clause (evita falha silenciosa com 500k IDs)
            _in_match = _re.search(
                r"(meta_ano\s*=\s*\d+)\s+AND\s+id_processo\s+IN\s*\((.+)\)\s*$",
                delete_condition, _re.DOTALL | _re.IGNORECASE
            )
            if _in_match and delete_condition.count(",") > 500:
                _meta_cond = _in_match.group(1).strip()
                _id_list   = [s.strip().strip("'\"")
                               for s in _in_match.group(2).split(",") if s.strip()]
                logger.info(
                    f"Deletando via temp table: {len(_id_list):,} IDs de "
                    f"{schema}.{table_name} WHERE {_meta_cond}"
                )
                with conn_driver.cursor() as cur:
                    cur.execute(
                        "CREATE TEMP TABLE _del_ids (id_processo varchar(50)) "
                        "ON COMMIT DELETE ROWS"
                    )
                    buf = _io.StringIO("\n".join(_id_list))
                    cur.copy_expert("COPY _del_ids (id_processo) FROM STDIN", buf)
                    cur.execute(
                        f"DELETE FROM {schema}.{table_name} t "
                        f"USING _del_ids d "
                        f"WHERE {_meta_cond} AND t.id_processo = d.id_processo"
                    )
                    logger.info(
                        f"Deletados {cur.rowcount:,} rows de {schema}.{table_name}"
                    )
                    cur.execute("DROP TABLE IF EXISTS _del_ids")
            else:
                with conn_driver.cursor() as cur:
                    delete_sql = f"DELETE FROM {schema}.{table_name} WHERE {delete_condition}"
                    logger.info(f"Executing pre-insert delete: {delete_sql}")
                    cur.execute(delete_sql)
                    logger.info(f"Deleted {cur.rowcount:,} rows from {schema}.{table_name}")
            conn_driver.close()
        except Exception as e:
            logger.warning(f"Could not delete from {schema}.{table_name} with condition '{delete_condition[:120]}': {e}")

    # Prepare write partition function that opens a connection inside the executor
    # Build small plain-Python objects on the driver to avoid serializing Spark internals
    cols = list(df.columns)
    # Choose expected types based on the target table's columns.
    # Two known shapes: `fato_auxiliar` and the `meta_calculator` meta table.
    cols_set = set(cols)
    meta_cols = {
        'id_processo', 'num_processo', 'cod_sistema', 'cod_classe_cnj',
        'id_assunto_principal', 'cod_competencia', 'dth_evento_indicador',
        'cod_indicador', 'id_sigilo', 'cod_natureza', 'dth_atualizacao',
        'cod_ult_oj', 'cod_ult_magistrado', 'meta_ano'
    }

    if meta_cols.issubset(cols_set):
        expected_types = {
            'id_processo': 'varchar50',
            'num_processo': 'varchar50',
            'cod_sistema': 'int2',
            'cod_classe_cnj': 'int4',
            'id_assunto_principal': 'int4',
            'cod_competencia': 'int4',
            'dth_evento_indicador': 'timestamp',
            'cod_indicador': 'int4',
            'id_sigilo': 'int4',
            'cod_natureza': 'int4',
            'dth_atualizacao': 'timestamp',
            'cod_ult_oj': 'numeric',
            'cod_ult_magistrado': 'numeric',
            'meta_ano': 'int4'
        }
        logger.info(f"Using meta table expected types for {schema}.{table_name}")
    else:
        # Default: fato_auxiliar mapping
        expected_types = {
            'id_processo': 'varchar50',
            'dth_prim_distribuicao': 'timestamp',
            'dth_ult_distribuicao': 'timestamp',
            'cod_prim_oj': 'int4',
            'cod_ult_oj': 'int4',
            'cod_prim_magistrado': 'int4',
            'cod_ult_magistrado': 'int4',
            'dth_ult_evento': 'timestamp',
            'cod_ult_evento_cnj': 'int4',
            'ind_baixado': 'int2',
            'ind_julgado': 'int2',
            'id_sigilo': 'int2',
            'complemento': 'text',
            'meta_ano': 'int4'
        }
        logger.info(f"Using fato_auxiliar expected types for {schema}.{table_name}")
    # Only insert columns that are both in the DataFrame and in the expected schema.
    # This prevents errors when the DataFrame contains extra columns (e.g. id_assunto_principal)
    # that have been removed from the target table.
    cols = [c for c in cols if c in expected_types]
    insert_sql = f"INSERT INTO {schema}.{table_name} ({', '.join(cols)}) VALUES %s"

    def write_partition(rows_iter):
        conn = None
        cur = None
        try:
            # IMPORTANT: open a new connection per partition (do not capture driver connections)
            conn = _connect_pg(pg_config)
            conn.autocommit = True
            cur = conn.cursor()
            batch = []
            batch_size = 5000
            for row in rows_iter:
                # Convert Row to tuple in column order
                try:
                    normalized = []
                    for c in cols:
                        v = None
                        try:
                            v = row[c]
                        except Exception:
                            # fallback if column access by name fails
                            # try positional access by index
                            try:
                                v = row[cols.index(c)]
                            except Exception:
                                v = None

                        t = expected_types.get(c)
                        # Trim strings and enforce varchar/text length
                        if t in ('varchar50', 'text') or isinstance(v, str):
                            if v is None:
                                norm = None
                            else:
                                s = str(v).strip()
                                if s == '':
                                    norm = None
                                else:
                                    if t == 'varchar50':
                                        norm = s[:50]
                                    else:
                                        norm = s
                        # Timestamps: try to preserve native datetime, or parse ISO-like strings
                        elif t == 'timestamp':
                            if v is None:
                                norm = None
                            elif isinstance(v, datetime):
                                norm = v
                            elif isinstance(v, str):
                                s = v.strip()
                                if s == '':
                                    norm = None
                                else:
                                    try:
                                        norm = datetime.fromisoformat(s)
                                    except Exception:
                                        norm = s
                            else:
                                # let psycopg2 handle other timestamp-like objects
                                norm = v
                        # Integer-like columns
                        elif t and t.startswith('int'):
                            if v is None:
                                norm = None
                            else:
                                # accept numeric types or numeric strings
                                try:
                                    if isinstance(v, str):
                                        s = v.strip()
                                        if s == '':
                                            norm = None
                                        else:
                                            norm = int(float(s))
                                    else:
                                        norm = int(v)
                                except Exception:
                                    # if casting fails, set NULL to avoid DB type errors
                                    norm = None
                        # Numeric/decimal columns
                        elif t == 'numeric':
                            if v is None:
                                norm = None
                            else:
                                try:
                                    if isinstance(v, str):
                                        s = v.strip()
                                        if s == '':
                                            norm = None
                                        else:
                                            norm = float(s)
                                    else:
                                        norm = float(v)
                                except Exception:
                                    norm = None
                        else:
                            # Default: pass value through (None, bool, float, etc.)
                            norm = v

                        normalized.append(norm)

                    tup = tuple(normalized)
                except Exception as e:
                    # fallback: use positional tuple
                    tup = tuple(row)
                batch.append(tup)
                if len(batch) >= batch_size:
                    psycopg2.extras.execute_values(cur, insert_sql, batch)
                    batch = []
            if batch:
                psycopg2.extras.execute_values(cur, insert_sql, batch)
        except Exception as e:
            # Print to executor logs
            print(f"Partition write failed: {e}")
            raise
        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            
    # Dynamic coalesce based on rowcount to limit concurrent connections
    try:
        try:
            row_count = df.count()
        except Exception:
            row_count = None
        import math
        if row_count is None:
            parts = 3
        else:
            parts = max(1, min(8, math.ceil(row_count / 200000)))
        logger.info(f"Writing {row_count if row_count is not None else 'unknown'} rows using {parts} partitions via psycopg2")
        df.coalesce(parts).rdd.foreachPartition(write_partition)
        logger.info(f"✅ Dados salvos com sucesso em {schema}.{table_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Distributed psycopg2 write failed: {e}")
        raise
    finally:
        try:
            conn_driver.close()
        except Exception:
            pass
