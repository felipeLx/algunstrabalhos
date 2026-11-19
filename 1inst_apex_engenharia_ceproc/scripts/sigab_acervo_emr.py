import sys
import json
import boto3
import psycopg2
import logging
from datetime import datetime, timedelta
import pytz
from pyspark.sql import SparkSession, Window
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
 s3://sgdai-projetos/1instancia/1inst_apex_engenharia_ceproc/scripts/sigab_acervo_emr.py  \
 --modo diario \
    2>&1 | \
   grep --line-buffered -E 'sigab_acervo|ERROR|Exception'



"""
# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
ID_PROJETO     = 10
CATALOG        = "glue_catalog"
DB_EJUD        = "ejud"
DB_EPROC       = "eproc2g"
DB_PRATA       = "dw_2inst_prata"
DB_OURO        = "dw_2inst_ouro"
DB_APEX        = "dw_apex"
AWS_REGION     = "sa-east-1"
FUSO_BR        = pytz.timezone("America/Sao_Paulo")
HOME_EXECUTOR  = "s3://sgdai-projetos/2instancia/2inst_sigab_acervo/scripts/sigab_acervo_emr.py"
TABLE_TARGET   = f"{CATALOG}.{DB_APEX}.sigab_acervo_hist"
COD_ORGAO_FILTER = 9187

# eproc2g MNI codes: levantamento de suspensão/sobrestamento
LEVANTAMENTO_CODES = [
    15414, 14982, 14981, 12066, 14974, 12067, 12068, 14980, 14979,
    14985, 15416, 14984, 14976, 14975, 14977, 14978, 14983,
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def tbl(db, name):
    return f"{CATALOG}.{db}.{name}"


def br_now_str():
    return datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")


def get_postgres_connection():
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    secret = json.loads(client.get_secret_value(SecretId="producao/rds/dw01/postgres")["SecretString"])
    return psycopg2.connect(
        host=secret["host"], database=secret["dbname"],
        user=secret["username"], password=secret["password"],
        port=int(secret["port"]),
    )


def registrar_inicio(dt_ref):
    conn = get_postgres_connection()
    agora = datetime.now(FUSO_BR).strftime("%Y-%m-%d %H:%M:%S.%f")
    try:
        with conn.cursor() as c:
            c.execute(
                "DELETE FROM dbcontrole.controle_execucao_carga_projeto "
                "WHERE id_projeto=%s AND schema_nome=%s AND tabela_nome=%s AND dt_referencia=%s",
                (ID_PROJETO, DB_APEX, "sigab_acervo", dt_ref),
            )
            c.execute(
                "INSERT INTO dbcontrole.controle_execucao_carga_projeto"
                "(id_projeto,schema_nome,tabela_nome,dt_carga,dt_ini_carga,status,home_script_executor,dt_referencia)"
                " VALUES(%s,%s,%s,%s,%s::timestamp,%s,%s,%s)",
                (ID_PROJETO, DB_APEX, "sigab_acervo", dt_ref.split(" ")[0],
                 agora, "INICIADA", HOME_EXECUTOR, dt_ref),
            )
            conn.commit()
    finally:
        conn.close()


def registrar_fim(dt_ref, qt, dt_ini, status, erro=None):
    conn = get_postgres_connection()
    agora = datetime.now(FUSO_BR)
    try:
        with conn.cursor() as c:
            c.execute(
                "UPDATE dbcontrole.controle_execucao_carga_projeto "
                "SET status=%s,dt_fim_carga=%s::timestamp,qt_registros=%s,"
                "tempo_execucao=%s,mensagem_erro=%s "
                "WHERE id_projeto=%s AND schema_nome=%s AND tabela_nome=%s AND dt_referencia=%s",
                (status, agora.strftime("%Y-%m-%d %H:%M:%S.%f"), qt,
                 str(agora - dt_ini).split(".")[0], erro,
                 ID_PROJETO, DB_APEX, "sigab_acervo", dt_ref),
            )
            conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# EJUD BASE
# ---------------------------------------------------------------------------

def build_ejud_base(spark):
    processo      = spark.table(tbl(DB_EJUD, "t_processo"))
    tipoprocesso  = spark.table(tbl(DB_EJUD, "tipoprocesso"))
    proc_assunto  = spark.table(tbl(DB_EJUD, "processoassunto"))
    movdistr      = spark.table(tbl(DB_EJUD, "movdistribuicao"))
    movcancedistr = spark.table(tbl(DB_EJUD, "movcancedistr"))
    movimento     = spark.table(tbl(DB_EJUD, "movimento"))
    movsobrest    = spark.table(tbl(DB_EJUD, "movsobrestamento"))
    assoc_local   = spark.table(tbl(DB_PRATA, "tb_associacao_localfisico"))
    assoc_mag     = spark.table(tbl(DB_PRATA, "tb_associacao_magistrado"))
    indicadores   = spark.table(tbl(DB_OURO, "tb_ft_acervo_indicadores"))
    situacao      = spark.table(tbl(DB_OURO, "tb_tf_situacao_ejud"))

    w_coddoc = Window.partitionBy("coddoc").orderBy(F.desc("dthrmov"))

    # 1. Use the same EJUD orgao/relator source already used by the historical SQL
    latest_rel = (
        indicadores
        .filter(F.col("ind_sistema") == "EJUD")
        .join(
            assoc_local.filter(
                (F.col("sistema") == "EJUD")
                & (F.col("codlocalparametrizado") == COD_ORGAO_FILTER)
            ).select("codlocalsistema", "codlocalparametrizado"),
            F.trim(F.col("cod_local_ultimo").cast("string")) == F.trim(F.col("codlocalsistema").cast("string")),
            "inner",
        )
        .select(
            F.col("cod_documento").cast("decimal(30,0)").alias("id_processo"),
            F.col("cod_magistrado_relator_ultimo").alias("codmagrel"),
            F.col("codlocalparametrizado").cast("decimal(10,0)").alias("cd_orgao"),
        )
        .dropDuplicates(["id_processo"])
    )

    # 2. CPF do relator
    mag_cpf = (
        assoc_mag.filter(F.col("ind_sistema") == "EJUD")
        .select("cod_magistrado", "cpf")
        .dropDuplicates(["cod_magistrado"])
    )

    # 3. Processo base + cod_cnj + classe CNJ
    base_proc = (
        processo
        .join(
            tipoprocesso.select("codtipproc", F.col("codextcnj").cast("int").alias("cd_classe")),
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
            F.col("codcompt").cast("int").alias("cd_competencia"),
            F.col("dtautua").cast("timestamp").alias("dt_autuacao"),
            "cd_classe",
        )
    )

    # 4. Assunto principal (numord = 1)
    assunto = (
        proc_assunto.filter(F.col("numord") == 1)
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.col("codass").cast("int").alias("cd_assunto"),
        )
    )

    # 5. Última distribuição válida
    cancelled = movcancedistr.select("coddoc", "dthrmovdistrcance")
    valid_distr = (
        movdistr.filter(F.col("codfase").isin(26, 36, 60012))
        .join(
            cancelled,
            (movdistr["coddoc"] == cancelled["coddoc"])
            & (movdistr["dthrmov"] == cancelled["dthrmovdistrcance"]),
            "left_anti",
        )
        .withColumn("rn", F.row_number().over(w_coddoc))
        .filter("rn = 1")
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.col("dthrmov").cast("timestamp").alias("dt_distribuicao"),
        )
    )

    # 6. Último movimento
    latest_mov = (
        movimento
        .withColumn("rn", F.row_number().over(w_coddoc))
        .filter("rn = 1")
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.col("codfase").cast("int").alias("cd_movimento"),
            F.col("dthrmov").cast("timestamp").alias("dt_movimento"),
        )
    )

    # 7. Última conclusão (codfase = 51)
    conclusao = (
        movimento.filter(F.col("codfase") == 51)
        .withColumn("rn", F.row_number().over(w_coddoc))
        .filter("rn = 1")
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.col("dthrmov").cast("timestamp").alias("dt_conclusao"),
        )
    )

    # 8. Suspensão ativa (movsobrestamento.dthrterminosobre IS NULL)
    susp = (
        movsobrest.filter(F.col("dthrterminosobre").isNull())
        .groupBy("coddoc")
        .agg(F.max(F.col("dthrmov").cast("timestamp")).alias("dt_suspensao"))
        .select(F.col("coddoc").cast("decimal(30,0)").alias("id_processo"), "dt_suspensao")
    )

    # 9. CNJ movement (latest) + in_julgado from tb_tf_situacao_ejud
    w_sit = Window.partitionBy("coddoc").orderBy(F.desc("dthrmov"))
    sit_latest = (
        situacao
        .withColumn("rn", F.row_number().over(w_sit))
        .filter("rn = 1")
        .select(
            F.col("coddoc").cast("decimal(30,0)").alias("id_processo"),
            F.col("cod_tip_mov_numero").cast("int").alias("cd_movimento_cnj"),
            F.col("dthrmov").cast("timestamp").alias("dt_movimento_cnj"),
        )
    )
    julgado = (
        situacao.filter(F.col("cod_tip_mov_numero") == 193)
        .select(F.col("coddoc").cast("decimal(30,0)").alias("id_processo"))
        .distinct()
        .withColumn("_julgado", F.lit(1))
    )

    now_str = F.lit(br_now_str())

    return (
        base_proc
        .join(latest_rel, base_proc["id_processo"] == latest_rel["id_processo"])
        .join(
            mag_cpf.select(F.col("cod_magistrado").alias("_cod_mag"), "cpf"),
            F.col("codmagrel") == F.col("_cod_mag"), "left",
        )
        .join(assunto,     base_proc["id_processo"] == assunto["id_processo"],      "left")
        .join(valid_distr, base_proc["id_processo"] == valid_distr["id_processo"])
        .join(latest_mov,  base_proc["id_processo"] == latest_mov["id_processo"],   "left")
        .join(conclusao,   base_proc["id_processo"] == conclusao["id_processo"],    "left")
        .join(susp,        base_proc["id_processo"] == susp["id_processo"],         "left")
        .join(sit_latest,  base_proc["id_processo"] == sit_latest["id_processo"],   "left")
        .join(julgado,     base_proc["id_processo"] == julgado["id_processo"],      "left")
        .select(
            F.lit("EJUD").alias("sistema"),
            base_proc["id_processo"],
            F.col("cod_cnj"),
            F.col("cpf").alias("cpf_relator"),
            F.col("cd_orgao"),
            F.col("cd_competencia"),
            F.col("cd_assunto"),
            F.col("cd_classe"),
            F.col("cd_movimento"),
            F.col("cd_movimento_cnj"),
            F.when(susp["dt_suspensao"].isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("int").alias("in_suspenso"),
            F.when(conclusao["dt_conclusao"].isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("int").alias("in_concluso"),
            F.when(julgado["_julgado"].isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("int").alias("in_julgado"),
            F.lit(1).cast("int").alias("in_acervo"),
            F.col("dt_movimento"),
            F.col("dt_movimento_cnj"),
            F.col("dt_distribuicao"),
            conclusao["dt_conclusao"],
            F.col("dt_autuacao"),
            susp["dt_suspensao"],
            now_str.alias("dt_criacao"),
            now_str.alias("dt_atualizacao"),
        )
    )


# ---------------------------------------------------------------------------
# EPROC2G BASE
# ---------------------------------------------------------------------------

def build_eproc_base(spark):
    processo    = spark.table(tbl(DB_EPROC, "processo"))
    proc_evento = spark.table(tbl(DB_EPROC, "processo_evento"))
    evento_jud  = spark.table(tbl(DB_EPROC, "evento_judicial"))
    hist_distr  = spark.table(tbl(DB_EPROC, "historico_distribuicao_judicial"))
    assunto_jud = spark.table(tbl(DB_EPROC, "assunto_judicial"))
    rel_classe  = spark.table(tbl(DB_EPROC, "relacao_classe_judicial_mni"))
    mni_mov     = spark.table(tbl(DB_EPROC, "mni_movimento"))
    assoc_local = spark.table(tbl(DB_PRATA, "tb_associacao_localfisico"))
    assoc_mag   = spark.table(tbl(DB_PRATA, "tb_associacao_magistrado"))
    situacao    = spark.table(tbl(DB_OURO, "tb_tf_situacao_eproc"))

    w_proc = Window.partitionBy("id_processo").orderBy(F.desc("dth_evento"))

    # Active events with MNI codes
    pe_mni = (
        proc_evento
        .filter(F.col("dth_desativacao").isNull() | (F.col("id_evento_judicial") != 40400400))
        .join(evento_jud.select("id_evento_judicial", "cod_mni_movimento"), "id_evento_judicial")
    )

    # 1. Filter processes to COD_ORGAO_FILTER via id_orgao_secretaria
    assoc_eproc = (
        assoc_local
        .filter((F.col("sistema") == "EPROC2G") & (F.col("codlocalparametrizado") == COD_ORGAO_FILTER))
        .select("codlocalsistema", F.col("codlocalparametrizado").cast("decimal(10,0)").alias("cd_orgao"))
    )
    proc_filtered = (
        processo
        .join(assoc_eproc, F.col("id_orgao_secretaria") == F.col("codlocalsistema"))
        .select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            "num_processo", "cod_competencia", "id_assunto_principal",
            "id_classe_judicial",
            F.col("dth_autuacao").cast("timestamp").alias("dt_autuacao"),
            "cd_orgao",
        )
    )

    # 2. Assunto CNJ
    assunto = assunto_jud.select(
        "id_assunto_judicial",
        F.col("cod_mni_assunto_judicial").cast("int").alias("cd_assunto"),
    )

    # 3. Classe CNJ
    classe = rel_classe.select(
        "id_classe_judicial",
        F.col("cod_mni_classe_judicial").cast("int").alias("cd_classe"),
    )

    # 4. Última distribuição válida + relator
    dist_evts     = pe_mni.filter(F.col("cod_mni_movimento").isin(26, 36))
    canc_evts     = pe_mni.filter(F.col("cod_mni_movimento").isin(488, 83))
    canc_for_join = canc_evts.select("id_processo", F.col("dth_evento").alias("dth_cancel"))
    valid_dist = (
        dist_evts
        .join(
            canc_for_join,
            (dist_evts["id_processo"] == canc_for_join["id_processo"])
            & (canc_for_join["dth_cancel"] > dist_evts["dth_evento"]),
            "left_anti",
        )
        .withColumn("rn", F.row_number().over(w_proc))
        .filter("rn = 1")
        .join(
            hist_distr.select("id_processo", "id_processo_evento", "cod_magistrado")
            .dropDuplicates(["id_processo", "id_processo_evento"]),
            ["id_processo", "id_processo_evento"], "left",
        )
        .select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            F.col("dth_evento").cast("timestamp").alias("dt_distribuicao"),
            F.col("cod_magistrado"),
        )
    )

    # 5. CPF do relator
    mag_cpf = (
        assoc_mag.filter(F.col("ind_sistema") == "EPROC2G")
        .select("cod_magistrado", "cpf")
        .dropDuplicates(["cod_magistrado"])
    )

    # 6. Último movimento
    latest_mov = (
        pe_mni
        .withColumn("rn", F.row_number().over(w_proc))
        .filter("rn = 1")
        .select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            F.col("id_evento_judicial").cast("int").alias("cd_movimento"),
            F.col("dth_evento").cast("timestamp").alias("dt_movimento"),
        )
    )

    # 7. Última conclusão (cod_mni_movimento = 51)
    conclusao = (
        pe_mni.filter(F.col("cod_mni_movimento") == 51)
        .withColumn("rn", F.row_number().over(w_proc))
        .filter("rn = 1")
        .select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            F.col("dth_evento").cast("timestamp").alias("dt_conclusao"),
        )
    )

    # 8. Suspensão ativa: cod_mni_movimento_pai = 25, sem levantamento posterior
    susp_codes = (
        mni_mov.filter(F.col("cod_mni_movimento_pai") == 25)
        .select(F.col("cod_mni_movimento").alias("_susp_mni"))
    )
    susp_evts = (
        pe_mni
        .join(susp_codes, F.col("cod_mni_movimento") == F.col("_susp_mni"))
        .withColumn("rn", F.row_number().over(w_proc))
        .filter("rn = 1")
        .select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            F.col("dth_evento").alias("dth_susp"),
        )
    )
    levant_evts = (
        pe_mni.filter(F.col("cod_mni_movimento").isin(LEVANTAMENTO_CODES))
        .select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            F.col("dth_evento").alias("dth_levant"),
        )
    )
    susp_valid = (
        susp_evts.alias("sv")
        .join(
            levant_evts.alias("lv"),
            (F.col("sv.id_processo") == F.col("lv.id_processo"))
            & (F.col("lv.dth_levant") > F.col("sv.dth_susp")),
            "left_anti",
        )
        .select(
            F.col("id_processo"),
            F.col("dth_susp").cast("timestamp").alias("dt_suspensao"),
        )
    )

    # 9. CNJ movement (latest) + in_julgado from tb_tf_situacao_eproc
    w_sit_eproc = Window.partitionBy("id_processo").orderBy(F.desc("dth_inicio"))
    sit_latest = (
        situacao
        .withColumn("rn", F.row_number().over(w_sit_eproc))
        .filter("rn = 1")
        .select(
            F.col("id_processo").cast("decimal(30,0)").alias("id_processo"),
            F.col("cod_mni_movimento").cast("int").alias("cd_movimento_cnj"),
            F.col("dth_inicio").cast("timestamp").alias("dt_movimento_cnj"),
        )
    )
    julgado = (
        situacao.filter(F.col("cod_mni_movimento") == 193)
        .select(F.col("id_processo").cast("decimal(30,0)").alias("id_processo"))
        .distinct()
        .withColumn("_julgado", F.lit(1))
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

    now_str = F.lit(br_now_str())

    return (
        proc_filtered
        .join(assunto, F.col("id_assunto_principal") == F.col("id_assunto_judicial"), "left")
        .join(classe, "id_classe_judicial", "left")
        .join(valid_dist,  proc_filtered["id_processo"] == valid_dist["id_processo"])
        .join(
            mag_cpf.select(F.col("cod_magistrado").alias("_cod_mag_e"), "cpf"),
            F.col("cod_magistrado") == F.col("_cod_mag_e"), "left",
        )
        .join(latest_mov, proc_filtered["id_processo"] == latest_mov["id_processo"],  "left")
        .join(conclusao,  proc_filtered["id_processo"] == conclusao["id_processo"],   "left")
        .join(susp_valid, proc_filtered["id_processo"] == susp_valid["id_processo"],  "left")
        .join(sit_latest, proc_filtered["id_processo"] == sit_latest["id_processo"],  "left")
        .join(julgado,    proc_filtered["id_processo"] == julgado["id_processo"],     "left")
        .select(
            F.lit("EPROC2G").alias("sistema"),
            proc_filtered["id_processo"],
            cod_cnj_expr.alias("cod_cnj"),
            F.col("cpf").alias("cpf_relator"),
            F.col("cd_orgao"),
            F.col("cod_competencia").cast("int").alias("cd_competencia"),
            F.col("cd_assunto"),
            F.col("cd_classe"),
            F.col("cd_movimento"),
            F.col("cd_movimento_cnj"),
            F.when(susp_valid["dt_suspensao"].isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("int").alias("in_suspenso"),
            F.when(conclusao["dt_conclusao"].isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("int").alias("in_concluso"),
            F.when(julgado["_julgado"].isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("int").alias("in_julgado"),
            F.lit(1).cast("int").alias("in_acervo"),
            F.col("dt_movimento"),
            F.col("dt_movimento_cnj"),
            F.col("dt_distribuicao"),
            conclusao["dt_conclusao"],
            F.col("dt_autuacao"),
            susp_valid["dt_suspensao"],
            now_str.alias("dt_criacao"),
            now_str.alias("dt_atualizacao"),
        )
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
dt_ini       = datetime.now(FUSO_BR)
mode         = "diario"
ref_date_arg = None

if "--modo" in sys.argv:
    mode = sys.argv[sys.argv.index("--modo") + 1]
if "--ref_date" in sys.argv:
    ref_date_arg = sys.argv[sys.argv.index("--ref_date") + 1]

if mode == "historico":
    raw = (ref_date_arg or (datetime.now(FUSO_BR) - timedelta(days=1)).strftime("%Y-%m-%d")).strip()
    v_dateref_str = f"{raw} 23:59:59" if len(raw) == 10 else raw
else:
    v_dateref_str = datetime.now(FUSO_BR).strftime("%Y-%m-%d %H:%M:%S")

logger.info(f"mode={mode} ref={v_dateref_str}")
registrar_inicio(v_dateref_str)

spark = (
    SparkSession.builder
    .appName(f"Sigab_Acervo_{mode}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog",              "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

total  = 0
status = "SUCESSO"
erro   = None

try:
    df_ejud  = build_ejud_base(spark)
    df_eproc = build_eproc_base(spark)
    df_base  = df_ejud.unionByName(df_eproc)
    ref_date = v_dateref_str.split(" ")[0]

    # Keep one snapshot per reference day: delete the previous run for the day, then append the new snapshot.
    df_final = (
        df_base
        .withColumn("rn", F.lit(1).cast("int"))
        .withColumn("dt_hr_carga", F.to_timestamp(F.lit(v_dateref_str)))
    )

    if spark.catalog.tableExists(TABLE_TARGET):
        spark.sql(
            f"DELETE FROM {TABLE_TARGET} WHERE CAST(dt_hr_carga AS DATE) = DATE '{ref_date}'"
        )
        df_final.writeTo(TABLE_TARGET).append()
    else:
        df_final.writeTo(TABLE_TARGET).using("iceberg").create()

    total = df_final.count()
    logger.info(f"Wrote {total} records to {TABLE_TARGET}")

except Exception as e:
    status = "ERRO"
    erro   = str(e)
    logger.error(f"Fatal: {erro}")
    raise

finally:
    registrar_fim(v_dateref_str, total, dt_ini, status, erro)
    spark.stop()
