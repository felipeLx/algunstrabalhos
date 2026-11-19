import sys
import logging
from datetime import datetime, timedelta, timezone
from functools import reduce
try:
    from zoneinfo import ZoneInfo
    _ZONE = ZoneInfo("America/Sao_Paulo")
    def _now_br():
        return datetime.now(_ZONE)
except ImportError:
    import pytz
    _ZONE = pytz.timezone("America/Sao_Paulo")
    def _now_br():
        return datetime.now(_ZONE)
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

"""
Como usar:
 spark-submit   --master yarn   --deploy-mode client  \
 --num-executors 10   --executor-memory 8G  \
 --executor-cores 4   --driver-memory 4G \
 --conf spark.sql.shuffle.partitions=200  \
 --conf spark.sql.session.timeZone=America/Sao_Paulo \
 s3://sgdai-projetos/apex/apex_sigab/scripts/sigab_acervo_emr.py \
 --modo diario \
    2>&1 | \
   grep --line-buffered -E 'sigab_acervo|ERROR|Exception'
"""

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
CATALOG          = "glue_catalog"
DB_DIM           = "dim_2inst_ouro"
DB_EPROC         = "eproc2g"
DB_PRATA         = "dw_2inst_prata"
DB_OURO          = "dw_2inst_ouro"
DB_APEX          = "dw_apex"
TABLE_TARGET     = f"{CATALOG}.{DB_APEX}.sigab_acervo_hist"
COD_ORGAO_FILTER = 9187

# Columns used to detect changes between snapshots (mirrors cte_merge WHERE clause)
COMPARE_COLS = [
    "cd_orgao", "cpf_relator", "cd_competencia", "cd_assunto", "cd_classe",
    "cd_movimento", "cd_movimento_cnj", "in_suspenso", "in_concluso", "in_julgado",
    "dt_movimento", "dt_movimento_cnj", "dt_distribuicao", "dt_conclusao",
    "dt_autuacao", "dt_suspensao",
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def tbl(db, name):
    return f"{CATALOG}.{db}.{name}"


def br_now_str():
    return _now_br().strftime("%Y-%m-%d %H:%M:%S")


def resolve_reference_datetime(mode="diario", ref_date_arg=None):
    if mode == "historico":
        raw = (ref_date_arg or (_now_br() - timedelta(days=1)).strftime("%Y-%m-%d")).strip()
        return f"{raw} 23:59:59" if len(raw) == 10 else raw
    return _now_br().strftime("%Y-%m-%d %H:%M:%S")


def parse_cli_args(argv=None):
    argv = argv or sys.argv
    mode = "diario"
    ref_date_arg = None
    if "--modo" in argv:
        mode = argv[argv.index("--modo") + 1]
    if "--ref_date" in argv:
        ref_date_arg = argv[argv.index("--ref_date") + 1]
    # --schema / --table passed by generic DAG; ignored here, script is self-contained
    return mode, ref_date_arg


# ---------------------------------------------------------------------------
# BASE — mirrors cte_base from sigab_acervo_hist.sql
# ---------------------------------------------------------------------------

def build_base(spark):
    pro = spark.table(tbl(DB_DIM, "tb_dim_processo"))
    ind = spark.table(tbl(DB_OURO, "tb_ft_acervo_indicadores"))
    org = (
        spark.table(tbl(DB_PRATA, "tb_associacao_localfisico"))
        .filter(F.col("codlocalparametrizado") == COD_ORGAO_FILTER)
    )
    mag = (
        spark.table(tbl(DB_PRATA, "tb_associacao_magistrado"))
        .select("cod_magistrado", "cpf", "ind_sistema")
        .distinct()
    )
    ass = (
        spark.table(tbl(DB_EPROC, "processo"))
        .select(
            F.col("id_processo").alias("_ass_id"),
            F.col("id_assunto_principal").alias("_ass_assunto"),
        )
    )

    now_ts = F.to_timestamp(F.lit(br_now_str()), "yyyy-MM-dd HH:mm:ss")

    return (
        pro.alias("pro")
        .join(
            ind.alias("ind"),
            F.col("ind.cod_documento") == F.col("pro.cod_documento"),
        )
        .join(
            org.alias("org"),
            (F.col("org.codlocalsistema") == F.col("ind.cod_local_ultimo"))
            & (F.col("org.sistema") == F.col("ind.ind_sistema")),
        )
        .join(
            mag.alias("mag"),
            (F.col("mag.cod_magistrado") == F.col("ind.cod_magistrado_relator_ultimo"))
            & (F.col("mag.ind_sistema") == F.col("ind.ind_sistema")),
        )
        .join(
            ass,
            (F.col("_ass_id") == F.col("pro.cod_documento"))
            & (F.col("pro.ind_sistema") == "EPROC2G"),
            "left",
        )
        .select(
            F.col("pro.ind_sistema").alias("sistema"),
            F.col("pro.cod_documento").alias("id_processo"),
            F.col("pro.cod_cnj"),
            F.col("mag.cpf").alias("cpf_relator"),
            F.col("org.codlocalparametrizado").cast("double").cast("bigint").alias("cd_orgao"),
            F.col("pro.cod_competencia").cast("double").cast("bigint").alias("cd_competencia"),
            F.when(F.col("pro.ind_sistema") == "EJUD",    F.col("pro.cod_assunto_principal").cast("double"))
             .when(F.col("pro.ind_sistema") == "EPROC2G", F.col("_ass_assunto").cast("double"))
             .cast("bigint").alias("cd_assunto"),
            F.col("pro.cod_classe_sistema").cast("double").cast("bigint").alias("cd_classe"),
            F.col("ind.cod_movimento_ultimo").cast("double").cast("bigint").alias("cd_movimento"),
            F.col("ind.cod_movimento_ultimo_cnj").cast("double").cast("bigint").alias("cd_movimento_cnj"),
            F.when(F.col("ind.ind_suspenso")  == "S", 1).otherwise(0).cast("int").alias("in_suspenso"),
            F.when(F.col("ind.ind_conclusao") == "S", 1).otherwise(0).cast("int").alias("in_concluso"),
            F.when(F.col("ind.ind_julgamento") == "S", 1).otherwise(0).cast("int").alias("in_julgado"),
            F.lit(1).cast("int").alias("in_acervo"),
            F.col("ind.dth_movimento_ultimo").cast("timestamp").alias("dt_movimento"),
            F.col("ind.dth_movimento_ultimo_cnj").cast("timestamp").alias("dt_movimento_cnj"),
            F.col("ind.dth_distribuicao_ultima").cast("timestamp").alias("dt_distribuicao"),
            F.col("ind.dth_conclusao_ultima").cast("timestamp").alias("dt_conclusao"),
            F.col("pro.dth_autuacao").cast("timestamp").alias("dt_autuacao"),
            F.col("ind.dth_suspensao").cast("timestamp").alias("dt_suspensao"),
            now_ts.alias("dt_criacao"),
            now_ts.alias("dt_atualizacao"),
        )
    )


# ---------------------------------------------------------------------------
# MERGE — mirrors cte_merge from sigab_acervo_hist.sql
# ---------------------------------------------------------------------------

def build_merge(df_base, df_antigos):
    """
    Returns delta rows only (new, changed, exited).
    Append this to the hist table — do NOT overwrite.
    """
    now_ts = F.to_timestamp(F.lit(br_now_str()), "yyyy-MM-dd HH:mm:ss")

    b = df_base.alias("b")
    s = df_antigos.alias("s")

    is_changed = reduce(
        lambda acc, c: acc | ~F.col(f"b.{c}").eqNullSafe(F.col(f"s.{c}")),
        COMPARE_COLS,
        F.lit(False),
    )

    joined = b.join(
        s,
        (F.col("b.id_processo") == F.col("s.id_processo"))
        & (F.col("b.cod_cnj") == F.col("s.cod_cnj")),
        "left",
    )

    # New rows or changed rows — mirrors first half of cte_merge UNION
    df_new_changed = (
        joined
        .filter(F.col("s.id_processo").isNull() | is_changed)
        .select(
            F.col("b.sistema"),
            F.col("b.id_processo"),
            F.col("b.cod_cnj"),
            F.col("b.cpf_relator"),
            F.col("b.cd_orgao"),
            F.col("b.cd_competencia"),
            F.col("b.cd_assunto"),
            F.col("b.cd_classe"),
            F.col("b.cd_movimento"),
            F.col("b.cd_movimento_cnj"),
            F.col("b.in_suspenso"),
            F.col("b.in_concluso"),
            F.col("b.in_julgado"),
            F.col("b.in_acervo"),
            F.col("b.dt_movimento"),
            F.col("b.dt_movimento_cnj"),
            F.col("b.dt_distribuicao"),
            F.col("b.dt_conclusao"),
            F.col("b.dt_autuacao"),
            F.col("b.dt_suspensao"),
            F.coalesce(F.col("s.dt_criacao"), F.col("b.dt_criacao")).alias("dt_criacao"),
            F.when(F.col("s.id_processo").isNull(), F.col("b.dt_criacao"))
             .otherwise(now_ts).alias("dt_atualizacao"),
        )
    )

    # Rows that left the acervo — mirrors second half of cte_merge UNION
    df_exited = (
        s.join(
            b,
            (F.col("b.id_processo") == F.col("s.id_processo"))
            & (F.col("b.cod_cnj") == F.col("s.cod_cnj")),
            "left_anti",
        )
        .filter(F.col("s.in_acervo") == 1)
        .select(
            F.col("s.sistema"),
            F.col("s.id_processo"),
            F.col("s.cod_cnj"),
            F.col("s.cpf_relator"),
            F.col("s.cd_orgao"),
            F.col("s.cd_competencia"),
            F.col("s.cd_assunto"),
            F.col("s.cd_classe"),
            F.col("s.cd_movimento"),
            F.col("s.cd_movimento_cnj"),
            F.col("s.in_suspenso"),
            F.col("s.in_concluso"),
            F.col("s.in_julgado"),
            F.lit(0).cast("int").alias("in_acervo"),
            F.col("s.dt_movimento"),
            F.col("s.dt_movimento_cnj"),
            F.col("s.dt_distribuicao"),
            F.col("s.dt_conclusao"),
            F.col("s.dt_autuacao"),
            F.col("s.dt_suspensao"),
            F.col("s.dt_criacao"),
            now_ts.alias("dt_atualizacao"),
        )
    )

    return df_new_changed.unionByName(df_exited)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(argv=None):
    mode, ref_date_arg = parse_cli_args(argv)
    v_dateref_str = resolve_reference_datetime(mode=mode, ref_date_arg=ref_date_arg)
    today = v_dateref_str.split(" ")[0]

    logger.info(f"mode={mode} ref={v_dateref_str}")

    spark = (
        SparkSession.builder
        .appName(f"Sigab_Acervo_{mode}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    df_final = None
    df_antigos = None
    total = 0

    try:
        df_base = build_base(spark)

        if spark.catalog.tableExists(TABLE_TARGET):
            df_antigos = spark.table(TABLE_TARGET).persist(StorageLevel.MEMORY_AND_DISK)
            df_final = build_merge(df_base, df_antigos).persist(StorageLevel.MEMORY_AND_DISK)
            total = df_final.count()
            # Idempotent re-run: remove today's delta before re-appending
            spark.sql(f"DELETE FROM {TABLE_TARGET} WHERE CAST(dt_atualizacao AS DATE) = DATE '{today}'")
            df_final.writeTo(TABLE_TARGET).append()
        else:
            df_final = df_base.persist(StorageLevel.MEMORY_AND_DISK)
            total = df_final.count()
            df_final.writeTo(TABLE_TARGET).using("iceberg").create()

        logger.info(f"Wrote {total} records to {TABLE_TARGET}")

    except Exception as e:
        logger.error(f"Fatal: {str(e)}")
        raise

    finally:
        for df in [df_final, df_antigos]:
            if df is not None:
                try:
                    df.unpersist()
                except Exception:
                    pass
        spark.stop()


if __name__ == "__main__":
    main()
