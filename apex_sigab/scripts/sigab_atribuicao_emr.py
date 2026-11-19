import sys
import logging
from datetime import datetime, timedelta
import pytz
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
 s3://sgdai-projetos/apex/apex_sigab/scripts/sigab_atribuicao_emr.py \
 --modo diario \
    2>&1 | \
   grep --line-buffered -E 'sigab_atribuicao|ERROR|Exception'
"""

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
CATALOG        = "glue_catalog"
DB_EJUD        = "ejud"
DB_EPROC       = "eproc2g"
DB_SEG         = "seg"
DB_PRATA       = "dw_2inst_prata"
DB_OURO        = "dw_2inst_ouro"
DB_APEX        = "dw_apex"
FUSO_BR        = pytz.timezone("America/Sao_Paulo")
TABLE_TARGET   = f"{CATALOG}.{DB_APEX}.sigab_atribuicao_hist"
S3_LOCATION    = "s3://sgdai-data-lake/ouro/dw_apex/sigab_atribuicao_hist"

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def tbl(db, name):
    return f"{CATALOG}.{db}.{name}"


def br_now_str():
    return datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")


def resolve_reference_datetime(mode="diario", ref_date_arg=None):
    if mode == "historico":
        raw = (ref_date_arg or (datetime.now(FUSO_BR) - timedelta(days=1)).strftime("%Y-%m-%d")).strip()
        return f"{raw} 23:59:59" if len(raw) == 10 else raw
    return datetime.now(FUSO_BR).strftime("%Y-%m-%d %H:%M:%S")


def parse_cli_args(argv=None):
    argv = argv or sys.argv
    mode = "diario"
    ref_date_arg = None

    if "--modo" in argv:
        mode = argv[argv.index("--modo") + 1]
    if "--ref_date" in argv:
        ref_date_arg = argv[argv.index("--ref_date") + 1]

    return mode, ref_date_arg


# ---------------------------------------------------------------------------
# EJUD BASE
# ---------------------------------------------------------------------------

def build_ejud_atrib(spark):
    # Bronze sources
    processo      = spark.table(tbl(DB_EJUD, "t_processo"))
    tipoprocesso  = spark.table(tbl(DB_EJUD, "tipoprocesso"))
    proc_assunto  = spark.table(tbl(DB_EJUD, "processoassunto"))
    sit_virt      = spark.table(tbl(DB_EJUD, "documentosituacaovirtual"))
    usuario       = spark.table(tbl(DB_SEG, "usuario"))
    # Kept sources
    indicadores   = spark.table(tbl(DB_OURO, "tb_ft_acervo_indicadores"))
    assoc_local   = spark.table(tbl(DB_PRATA, "tb_associacao_localfisico"))
    assoc_mag     = spark.table(tbl(DB_PRATA, "tb_associacao_magistrado"))
    sigab_acervo  = spark.table(tbl(DB_APEX, "sigab_acervo"))

    # Filter acervo to EJUD
    acervo_ejud = (
        sigab_acervo
        .filter(F.col("sistema") == "EJUD")
        .select(F.col("id_processo").cast("decimal(30,0)").alias("id_processo"))
    )

    # Attribution: documentosituacaovirtual + usuario → cpf_atribuido + dt_atribuicao
    atrib = (
        sit_virt
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.col("idusubloq").alias("cod_usu"),
            F.col("dthrentr").cast("timestamp").alias("dt_atribuicao"),
        )
        .join(
            usuario.select(
                F.col("cod_usu"),
                F.upper(F.trim(F.col("cpf_usu"))).alias("cpf_atribuido"),
            ),
            "cod_usu",
        )
    )

    # Indicators (EJUD)
    ind_ejud = (
        indicadores
        .filter(F.col("ind_sistema") == "EJUD")
        .select(
            F.col("cod_documento").cast("decimal(30,0)").alias("id_processo"),
            F.col("cod_movimento_ultimo").cast("bigint").alias("cd_movimento"),
            F.when(F.col("ind_suspenso") == "S", 1).otherwise(0).cast("int").alias("in_suspenso"),
            F.when(F.col("ind_conclusao") == "S", 1).otherwise(0).cast("int").alias("in_concluso"),
            F.when(F.col("ind_julgamento") == "S", 1).otherwise(0).cast("int").alias("in_julgado"),
            F.col("dth_movimento_ultimo").cast("timestamp").alias("dt_movimento"),
            F.col("dth_distribuicao_ultima").cast("timestamp").alias("dt_distribuicao"),
            F.col("dth_conclusao_ultima").cast("timestamp").alias("dt_conclusao"),
            F.col("dth_suspensao").cast("timestamp").alias("dt_suspensao"),
            F.col("cod_local_ultimo").alias("cod_local"),
            F.col("cod_magistrado_relator_ultimo").alias("cod_magistrado"),
            F.col("ind_sistema"),
        )
    )

    # Orgao lookup
    org = assoc_local.select(
        F.col("codlocalsistema"),
        F.col("codlocalparametrizado").cast("bigint").alias("cd_orgao"),
        F.col("sistema").alias("sistema_local"),
    )

    # Relator CPF (dedup by cod_magistrado)
    mag = (
        assoc_mag
        .filter(F.col("ind_sistema") == "EJUD")
        .select("cod_magistrado", F.col("cpf").alias("cpf_relator"), "ind_sistema")
        .dropDuplicates(["cod_magistrado"])
    )

    # Processo base + cod_cnj (bronze)
    base_proc = (
        processo
        .join(
            tipoprocesso.select(
                "codtipproc",
                F.col("codextcnj").cast("bigint").alias("cd_classe"),
            ),
            "codtipproc", "left",
        )
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.concat(
                F.lpad(F.col("numsequencial").cast("string"), 7, "0"), F.lit("-"),
                F.lpad(F.col("dv").cast("string"), 2, "0"),            F.lit("."),
                F.col("numano").cast("string"),                         F.lit("."),
                F.col("codjustica").cast("string"),                     F.lit("."),
                F.lpad(F.col("codregi").cast("string"), 2, "0"),        F.lit("."),
                F.lpad(F.col("codlocregi").cast("string"), 4, "0"),
            ).alias("cod_cnj"),
            F.col("codcompt").cast("bigint").alias("cd_competencia"),
            F.col("dtautua").cast("timestamp").alias("dt_autuacao"),
            "cd_classe",
        )
    )

    # Assunto principal (numord = 1)
    assunto = (
        proc_assunto
        .filter(F.col("numord") == 1)
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.col("codass").cast("bigint").alias("cd_assunto"),
        )
    )

    now_ts = F.to_timestamp(F.lit(br_now_str()), "yyyy-MM-dd HH:mm:ss")

    return (
        base_proc
        .join(acervo_ejud,  "id_processo")
        .join(atrib,        "id_processo")
        .join(ind_ejud,     "id_processo")
        .join(
            org,
            (ind_ejud["cod_local"] == org["codlocalsistema"])
            & (ind_ejud["ind_sistema"] == org["sistema_local"]),
            "left",
        )
        .join(mag, ind_ejud["cod_magistrado"] == mag["cod_magistrado"], "left")
        .join(assunto, base_proc["id_processo"] == assunto["id_processo"], "left")
        .filter(F.col("cpf_atribuido").isNotNull())
        .select(
            F.col("cpf_atribuido"),
            F.lit("EJUD").alias("sistema"),
            base_proc["id_processo"],
            F.col("cod_cnj"),
            F.col("cpf_relator"),
            F.col("cd_orgao"),
            F.col("cd_competencia"),
            F.col("cd_assunto"),
            F.col("cd_classe"),
            F.col("cd_movimento"),
            F.col("in_suspenso"),
            F.col("in_concluso"),
            F.col("in_julgado"),
            F.col("dt_atribuicao").cast("timestamp"),
            F.col("dt_movimento").cast("timestamp"),
            F.col("dt_distribuicao").cast("timestamp"),
            F.col("dt_conclusao").cast("timestamp"),
            F.col("dt_autuacao").cast("timestamp"),
            F.col("dt_suspensao").cast("timestamp"),
            F.lit("EJUD").alias("criado_por"),
            F.lit("EJUD").alias("atualizado_por"),
            now_ts.alias("dt_criacao"),
        )
    )


# ---------------------------------------------------------------------------
# EPROC2G BASE
# ---------------------------------------------------------------------------

def build_eproc_atrib(spark):
    # Bronze processo source (replaces dim_2inst_ouro.tb_dim_processo)
    processo    = spark.table(tbl(DB_EPROC, "processo"))
    assunto_jud = spark.table(tbl(DB_EPROC, "assunto_judicial"))
    rel_classe  = spark.table(tbl(DB_EPROC, "relacao_classe_judicial_mni"))
    # Kept sources
    indicadores  = spark.table(tbl(DB_OURO, "tb_ft_acervo_indicadores"))
    assoc_local  = spark.table(tbl(DB_PRATA, "tb_associacao_localfisico"))
    assoc_mag    = spark.table(tbl(DB_PRATA, "tb_associacao_magistrado"))
    sigab_acervo = spark.table(tbl(DB_APEX, "sigab_acervo"))
    localizador  = spark.table(tbl(DB_APEX, "tb_localizador_processo_2inst"))

    # Filter acervo to EPROC2G
    acervo_eproc = (
        sigab_acervo
        .filter(F.col("sistema") == "EPROC2G")
        .select(F.col("id_processo").cast("decimal(30,0)").alias("id_processo"))
    )

    # Localizador + CPF mapping (matches original CASE WHEN logic)
    loc_with_cpf = (
        localizador
        .join(acervo_eproc, localizador["id_processo"] == acervo_eproc["id_processo"])
        .withColumn("sig_loc_clean", F.regexp_replace(F.col("sig_localizador"), r"\s*/\s*", "/"))
        .withColumn(
            "cpf_atribuido",
            F.upper(F.trim(
                F.when(F.col("sig_loc_clean").like("%1CM%"),  F.lit("07742791765"))
                .when(F.col("sig_loc_clean").like("%2MM%"),  F.lit("11490803700"))
                .when(F.col("sig_loc_clean").like("%3CNT%"), F.lit("04145262794"))
                .when(F.col("sig_loc_clean").like("%4MA%"),  F.lit("05523089788"))
                .when(F.col("sig_loc_clean").like("%9IQ%"),  F.lit("10206089783"))
                .when(F.col("sig_loc_clean").like("%10IP%"), F.lit("08457355783"))
                .when(F.col("sig_loc_clean").like("%11PS%"), F.lit("08331604784"))
                .when(F.col("sig_loc_clean").like("%12BS%"), F.lit("05509570733"))
            ))
        )
        .filter(F.col("cpf_atribuido").isNotNull())
        .select(
            localizador["id_processo"].cast("decimal(30,0)").alias("id_processo"),
            F.col("cpf_atribuido"),
        )
    )

    # Indicators (EPROC2G)
    ind_eproc = (
        indicadores
        .filter(F.col("ind_sistema") == "EPROC2G")
        .select(
            F.col("cod_documento").cast("decimal(30,0)").alias("id_processo"),
            F.col("cod_movimento_ultimo").cast("bigint").alias("cd_movimento"),
            F.when(F.col("ind_suspenso") == "S", 1).otherwise(0).cast("int").alias("in_suspenso"),
            F.when(F.col("ind_conclusao") == "S", 1).otherwise(0).cast("int").alias("in_concluso"),
            F.when(F.col("ind_julgamento") == "S", 1).otherwise(0).cast("int").alias("in_julgado"),
            F.col("dth_movimento_ultimo").cast("timestamp").alias("dt_movimento"),
            F.col("dth_distribuicao_ultima").cast("timestamp").alias("dt_distribuicao"),
            F.col("dth_conclusao_ultima").cast("timestamp").alias("dt_conclusao"),
            F.col("dth_suspensao").cast("timestamp").alias("dt_suspensao"),
            F.col("cod_local_ultimo").alias("cod_local"),
            F.col("cod_magistrado_relator_ultimo").alias("cod_magistrado"),
            F.col("ind_sistema"),
        )
    )

    # Orgao lookup
    org = assoc_local.select(
        F.col("codlocalsistema"),
        F.col("codlocalparametrizado").cast("bigint").alias("cd_orgao"),
        F.col("sistema").alias("sistema_local"),
    )

    # Relator CPF (dedup by cod_magistrado)
    mag = (
        assoc_mag
        .filter(F.col("ind_sistema") == "EPROC2G")
        .select("cod_magistrado", F.col("cpf").alias("cpf_relator"), "ind_sistema")
        .dropDuplicates(["cod_magistrado"])
    )

    # Assunto CNJ
    assunto = assunto_jud.select(
        "id_assunto_judicial",
        F.col("cod_mni_assunto_judicial").cast("bigint").alias("cd_assunto"),
    )

    # Classe CNJ
    classe = rel_classe.select(
        "id_classe_judicial",
        F.col("cod_mni_classe_judicial").cast("bigint").alias("cd_classe"),
    )

    # cod_cnj from num_processo (pad to 20 digits)
    num_pad = F.lpad(F.col("num_processo").cast("string"), 20, "0")
    cod_cnj_expr = F.concat(
        F.substring(num_pad, 1,  7), F.lit("-"),
        F.substring(num_pad, 8,  2), F.lit("."),
        F.substring(num_pad, 10, 4), F.lit("."),
        F.substring(num_pad, 14, 1), F.lit("."),
        F.substring(num_pad, 15, 2), F.lit("."),
        F.substring(num_pad, 17, 4),
    )

    proc_base = (
        processo.select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            "num_processo",
            F.col("cod_competencia").cast("bigint").alias("cd_competencia"),
            "id_assunto_principal",
            "id_classe_judicial",
            F.col("dth_autuacao").cast("timestamp").alias("dt_autuacao"),
        )
    )

    now_ts = F.to_timestamp(F.lit(br_now_str()), "yyyy-MM-dd HH:mm:ss")

    return (
        proc_base
        .join(loc_with_cpf, "id_processo")
        .join(ind_eproc,    "id_processo")
        .join(
            org,
            (ind_eproc["cod_local"] == org["codlocalsistema"])
            & (ind_eproc["ind_sistema"] == org["sistema_local"]),
            "left",
        )
        .join(mag, ind_eproc["cod_magistrado"] == mag["cod_magistrado"], "left")
        .join(assunto, F.col("id_assunto_principal") == F.col("id_assunto_judicial"), "left")
        .join(classe, "id_classe_judicial", "left")
        .select(
            F.col("cpf_atribuido"),
            F.lit("EPROC2G").alias("sistema"),
            proc_base["id_processo"],
            cod_cnj_expr.alias("cod_cnj"),
            F.col("cpf_relator"),
            F.col("cd_orgao"),
            F.col("cd_competencia"),
            F.col("cd_assunto"),
            F.col("cd_classe"),
            F.col("cd_movimento"),
            F.col("in_suspenso"),
            F.col("in_concluso"),
            F.col("in_julgado"),
            now_ts.cast("timestamp").alias("dt_atribuicao"),
            F.col("dt_movimento").cast("timestamp"),
            F.col("dt_distribuicao").cast("timestamp"),
            F.col("dt_conclusao").cast("timestamp"),
            F.col("dt_autuacao").cast("timestamp"),
            F.col("dt_suspensao").cast("timestamp"),
            F.lit("EPROC2G").alias("criado_por"),
            F.lit("EPROC2G").alias("atualizado_por"),
            now_ts.alias("dt_criacao"),
        )
    )


# ---------------------------------------------------------------------------
# BUILD DATAFRAME
# ---------------------------------------------------------------------------

def build_dataframe(spark, mode="diario", ref_date_arg=None):
    v_dateref_str = resolve_reference_datetime(mode=mode, ref_date_arg=ref_date_arg)
    df_ejud  = build_ejud_atrib(spark)
    df_eproc = build_eproc_atrib(spark)
    df_base  = df_ejud.unionByName(df_eproc)

    return df_base.select(
        "cpf_atribuido",
        "sistema",
        "id_processo",
        "cod_cnj",
        "cpf_relator",
        "cd_orgao",
        "cd_competencia",
        "cd_assunto",
        "cd_classe",
        "cd_movimento",
        "in_suspenso",
        "in_concluso",
        "in_julgado",
        F.col("dt_atribuicao").cast("timestamp"),
        F.col("dt_movimento").cast("timestamp"),
        F.col("dt_distribuicao").cast("timestamp"),
        F.col("dt_conclusao").cast("timestamp"),
        F.col("dt_autuacao").cast("timestamp"),
        F.col("dt_suspensao").cast("timestamp"),
        "criado_por",
        "atualizado_por",
        F.to_timestamp(F.lit(v_dateref_str), "yyyy-MM-dd HH:mm:ss").alias("dt_criacao"),
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(argv=None):
    dt_ini = datetime.now(FUSO_BR)
    mode, ref_date_arg = parse_cli_args(argv)
    v_dateref_str = resolve_reference_datetime(mode=mode, ref_date_arg=ref_date_arg)

    logger.info(f"mode={mode} ref={v_dateref_str}")

    spark = (
        SparkSession.builder
        .appName(f"Sigab_Atribuicao_{mode}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    total = 0
    df_final = None

    try:
        df_final = build_dataframe(spark, mode=mode, ref_date_arg=ref_date_arg)
        ref_date = v_dateref_str.split(" ")[0]

        df_final = df_final.persist(StorageLevel.MEMORY_AND_DISK)
        total = df_final.count()

        if spark.catalog.tableExists(TABLE_TARGET):
            spark.sql(
                f"DELETE FROM {TABLE_TARGET} WHERE CAST(dt_criacao AS DATE) = DATE '{ref_date}'"
            )
            df_final.writeTo(TABLE_TARGET).append()
        else:
            df_final.writeTo(TABLE_TARGET).using("iceberg") \
                .tableProperty("location", S3_LOCATION) \
                .create()

        logger.info(f"Wrote {total} records to {TABLE_TARGET}")

    except Exception as e:
        logger.error(f"Fatal: {str(e)}")
        raise

    finally:
        if df_final is not None:
            df_final.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
