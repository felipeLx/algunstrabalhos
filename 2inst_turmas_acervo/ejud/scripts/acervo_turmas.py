import os
import sys
import argparse
import traceback
from datetime import datetime
from dotenv import load_dotenv

PARQUET_HOME = os.environ.get("PARQUET_HOME", "/opt/parquet")
load_dotenv(dotenv_path=os.path.join(PARQUET_HOME, "airflow", "dotenv", ".env"))
sys.path.append(os.path.join(PARQUET_HOME, "airflow_2inst", "scripts"))
sys.path.insert(0, os.path.join(PARQUET_HOME, "airflow_2inst", "scripts"))

ambiente = os.environ.get("AMBIENTE", "local").lower()

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.storagelevel import StorageLevel
from modulo.conexoes import get_postgres_connection
from modulo.controle_carga import registrar_inicio_carga, registrar_fim_carga
from acervo_db_loader import carrega_turmas_relatorio_pg
from turmas_staged_builder import build_turmas_staged

def print_info(text):    print(f"INFO  {text}")
def print_success(text): print(f"OK    {text}")
def print_error(text):   print(f"ERROR {text}")
def print_warning(text): print(f"WARN  {text}")

# Turmas OJs — codorgjulg das turmas do TJRJ 2a instância
_TURMAS_ORGAOS = (681, 682, 3553, 3798, 3799, 3800, 6676, 6739, 6740, 9525, 9816, 9940)
_TURMAS_ORGAOS_STR = ", ".join(str(x) for x in _TURMAS_ORGAOS)

# Stage 1 query — base acervo turmas.
# Eligibility: proc_atual_stg (codcompt turmas + last codorgjulg IN turmas OJs).
# ultima_distribuicao restricted to distributions at turmas OJs (JOIN movimentorelator).
# LEFT JOIN — processes without qualifying distribution remain eligible (dataultdis = NULL).
# Output columns: coddoc, dataultdis, datareferencia
_SQL_TURMAS_BASE = f"""
WITH
ultima_distribuicao AS (
    SELECT coddoc, dthrmov AS dataultdis
    FROM (
        SELECT
            m.coddoc,
            m.dthrmov,
            ROW_NUMBER() OVER (PARTITION BY m.coddoc ORDER BY m.dthrmov DESC) AS rn
        FROM movimento m
        JOIN movimentorelator mr ON m.coddoc = mr.coddoc AND m.dthrmov = mr.dthrmov
        WHERE m.codfase IN (26, 36, 60012)
          AND m.dthrmov <= '{{v_dateref_str}}'
    ) t
    WHERE rn = 1
)
SELECT pa.coddoc, ud.dataultdis, '{{v_dateref_str}}' AS datareferencia
FROM proc_atual_stg pa
LEFT JOIN ultima_distribuicao ud ON pa.coddoc = ud.coddoc
"""


def _safe_trunc(msg, maxlen=4000):
    if msg is None:
        return None
    return msg if len(msg) <= maxlen else msg[:maxlen - 3] + "..."


def _create_spark():
    spark_dir = os.environ.get("SPARK_LOCAL_DIR")
    if not spark_dir and ambiente == "local":
        spark_dir = "/opt/parquet/tmp_spark"
    if spark_dir and not spark_dir.lower().startswith(("s3://", "s3a://", "hdfs://")):
        os.makedirs(spark_dir, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName("Acervo Turmas EJUD")
        .config("spark.driver.memory", "24g")
        .config("spark.executor.memory", "24g")
        .config("spark.executor.cores", "4")
        .config("spark.driver.cores", "2")
        .config("spark.driver.extraJavaOptions",
                "-XX:+UseG1GC -XX:G1HeapRegionSize=32m "
                "-XX:+UnlockDiagnosticVMOptions -XX:InitiatingHeapOccupancyPercent=35")
        .config("spark.executor.extraJavaOptions",
                "-XX:+UseG1GC -XX:G1HeapRegionSize=32m")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.memory.fraction", "0.6")
        .config("spark.executor.memoryOverhead", "4g")
        .config("spark.sql.files.maxPartitionBytes", "64MB")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    )
    if spark_dir:
        builder = builder.config("spark.local.dir", spark_dir)
    return builder.getOrCreate()


def main(dateref: str = None):
    id_projeto  = 17
    schema      = "dw2inst"
    tabela      = "turmas_relatorio_final"

    if dateref:
        dt_referencia = datetime.strptime(dateref, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        )
    else:
        dt_referencia = datetime.now()

    home_script = os.path.abspath(__file__)
    start_time  = datetime.now()

    registrar_inicio_carga(id_projeto, schema, tabela, dt_referencia, home_script)

    spark = None
    try:
        spark = _create_spark()

        v_dateref_str = dt_referencia.strftime("%Y-%m-%d %H:%M:%S")

        def parquet_path(folder):
            if ambiente == "local":
                return f"/opt/parquet/data/bronze/ejud/{folder}"
            return f"s3a://sgdai-data-lake/bronze/ejud/{folder}"

        tabelas_src = [
            "t_processo",
            "logprocesso",
            "movimentofisico",
            "movimento",
            "movimentorelator",
            "magistrado",
            "pessoa",
            "orgaojulgador",
            "competencia",
            "localvirtualdocumento",
            "localvirtual",
            "processoassunto",
            "assunto",
            "autoassociado",
            "vara",
            "parte",
            "parteprocesso",
            "tipoparte",
            "movsobrestamento",
            "fase",
            "faseexterna",
            "complemento",
            "tipoprocesso",
            "localfisico",
        ]

        print_info("Lendo Parquets e criando TempViews...")
        falhas = []
        for t in tabelas_src:
            try:
                df = (
                    spark.read
                    .option("mergeSchema", "false")
                    .option("pathGlobFilter", "*.parquet")
                    .parquet(parquet_path(t))
                )
                df.createOrReplaceTempView(t)
            except Exception as e:
                falhas.append((t, str(e)))
                print_warning(f"Nao foi possivel carregar '{t}': {e}")

        if falhas:
            raise RuntimeError(
                "Falha ao carregar tabelas: " +
                ", ".join(f"{t}: {m}" for t, m in falhas[:5]) +
                (" ..." if len(falhas) > 5 else "")
            )

        if ambiente == "local":
            output_turmas = os.path.join(PARQUET_HOME, "data", "ouro", "dw2inst", "turmas_relatorio_final")
        else:
            output_turmas = "s3a://sgdai-data-lake/ouro/dw2inst/turmas_relatorio_final"

        # Pre-stage 1: proc_base — t_processo filtered by codcompt + codtipproc
        print_info("Pre-stage 1: proc_base (codcompt/codtipproc filter)...")
        df_proc_base = spark.sql("""
            SELECT p.coddoc, p.codtipproc, p.codcompt
            FROM t_processo p
            WHERE p.codtipproc != 288
              AND p.codcompt IN (10, 11, 12, 14, 15, 19, 20, 22)
        """)
        df_proc_base.persist(StorageLevel.MEMORY_AND_DISK)
        qt_base = df_proc_base.count()
        df_proc_base.createOrReplaceTempView("proc_base_stg")
        print_success(f"proc_base_stg: {qt_base:,} processos.")

        # Pre-stage 2: last codorgjulg per coddoc from movimentorelator
        print_info("Pre-stage 2: last_orgaojulg_stg (last movimentorelator.codorgjulg)...")
        proc_base_coddocs = df_proc_base.select("coddoc")
        df_mr_base = (
            spark.table("movimentorelator")
            .join(broadcast(proc_base_coddocs), on="coddoc", how="inner")
            .filter(f"dthrmov <= '{v_dateref_str}'")
        )
        df_mr_base.persist(StorageLevel.MEMORY_AND_DISK)
        df_mr_base.count()
        df_mr_base.createOrReplaceTempView("movimentorelator_base_stg")

        df_last_oj = spark.sql("""
            SELECT coddoc, codorgjulg AS last_codorgjulg
            FROM (
                SELECT mr.coddoc, mr.codorgjulg,
                       ROW_NUMBER() OVER (PARTITION BY mr.coddoc ORDER BY mr.dthrmov DESC) AS rn
                FROM movimentorelator_base_stg mr
                JOIN orgaojulgador oj  ON mr.codorgjulg = oj.codorgjulg
                JOIN competencia   com ON oj.codcompt   = com.codcompt
                WHERE com.indconsrec = 'S'
            ) t WHERE rn = 1
        """)
        df_last_oj.persist(StorageLevel.MEMORY_AND_DISK)
        qt_oj = df_last_oj.count()
        df_last_oj.createOrReplaceTempView("last_orgaojulg_stg")
        print_success(f"last_orgaojulg_stg: {qt_oj:,} coddocs com movimentorelator.")

        # Pre-stage 3: proc_atual — proc_base filtered by turmas OJs
        print_info("Pre-stage 3: proc_atual (last codorgjulg IN turmas OJs)...")
        turmas_oj_str = ", ".join(str(x) for x in _TURMAS_ORGAOS)
        df_proc_atual = spark.sql(f"""
            SELECT pb.coddoc, pb.codtipproc, pb.codcompt
            FROM proc_base_stg pb
            JOIN last_orgaojulg_stg oj ON pb.coddoc = oj.coddoc
            WHERE oj.last_codorgjulg IN ({turmas_oj_str})
        """)
        df_proc_atual.persist(StorageLevel.MEMORY_AND_DISK)
        qt_proc = df_proc_atual.count()
        df_proc_atual.createOrReplaceTempView("proc_atual_stg")
        print_success(f"proc_atual_stg: {qt_proc:,} processos elegiveis.")

        proc_coddocs = df_proc_atual.select("coddoc")
        for tbl in ["movimentofisico", "movimento"]:
            df_filt = spark.table(tbl).join(broadcast(proc_coddocs), on="coddoc", how="inner")
            df_filt.persist(StorageLevel.MEMORY_AND_DISK)
            df_filt.count()
            df_filt.createOrReplaceTempView(tbl)
            print_success(f"  {tbl}: pre-filtrado por proc_atual ({qt_proc:,} coddocs).")

        print_info("Stage 1: filtro base de acervo turmas...")
        sql_base = _SQL_TURMAS_BASE.format(v_dateref_str=v_dateref_str)
        df_base = spark.sql(sql_base)
        df_base.persist(StorageLevel.MEMORY_AND_DISK)
        df_base.createOrReplaceTempView("turmas_base")
        qt_base = df_base.count()
        print_success(f"Stage 1 concluida: {qt_base} processos elegiveis.")

        print_info("Pre-filtrando tabelas pesadas para coddocs elegiveis...")
        coddocs_df = df_base.select("coddoc")

        # movimento: 6 CTEs in stage 2 — must cache to avoid re-scanning parquet 6x.
        # movimentorelator: 2 CTEs (mov_relator, primeiro_relator) — cache.
        for tbl in ["movimento", "movimentorelator"]:
            df_filtered = spark.table(tbl).join(broadcast(coddocs_df), on="coddoc", how="inner")
            df_filtered.persist(StorageLevel.MEMORY_AND_DISK)
            df_filtered.count()
            df_filtered.createOrReplaceTempView(tbl)
            print_success(f"  {tbl}: filtrado e persistido.")

        for tbl in ["movimentofisico", "logprocesso", "movsobrestamento",
                    "processoassunto", "localvirtualdocumento"]:
            df_filtered = spark.table(tbl).join(broadcast(coddocs_df), on="coddoc", how="inner")
            df_filtered.createOrReplaceTempView(tbl)
            print_success(f"  {tbl}: filtrado.")

        print_info("Stage 2: relatorio turmas (staged)...")
        df_turmas = build_turmas_staged(spark, v_dateref_str, log=print_info)
        df_turmas.repartition(100).write.mode("overwrite").parquet(output_turmas)
        print_success(f"Parquet turmas gerado: {output_turmas}")

        carrega_turmas_relatorio_pg(output_turmas, batch_size=50000)

        conn = get_postgres_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw2inst.turmas_relatorio_final")
            qt_pg = cur.fetchone()[0]
        conn.close()

        registrar_fim_carga(
            id_projeto=id_projeto,
            schema=schema,
            tabela=tabela,
            dt_referencia=dt_referencia,
            dt_fim_carga=datetime.now(),
            qt_registros=qt_pg,
            status="SUCESSO",
            tempo_execucao=str(datetime.now() - start_time),
            mensagem_erro=None,
        )
        print_success(f"Carga turmas finalizada: {qt_pg} registros.")

    except Exception as e:
        tb = traceback.format_exc()
        print_error(f"Erro na carga de turmas: {e}")
        try:
            registrar_fim_carga(
                id_projeto=id_projeto,
                schema=schema,
                tabela=tabela,
                dt_referencia=dt_referencia,
                dt_fim_carga=datetime.now(),
                qt_registros=0,
                status="ERRO",
                tempo_execucao=str(datetime.now() - start_time),
                mensagem_erro=_safe_trunc(tb),
            )
        except Exception:
            pass
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dateref", default=None,
                    help="Reference date YYYY-MM-DD (default: today). "
                         "Query uses 23:59:59 of that day as cutoff.")
    args = ap.parse_args()
    main(dateref=args.dateref)
