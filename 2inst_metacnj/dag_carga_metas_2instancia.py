# -*- coding: utf-8 -*-
#
# Criacao - 2026-04-19 - dag_carga_metas_2instancia.py
#

import logging
from datetime import timedelta

import boto3
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

logger = logging.getLogger(__name__)

# =========================================================
# CONFIG
# =========================================================
DAG_ID = "dag_carga_metas_2instancia"
TZ = pendulum.timezone("America/Sao_Paulo")
CLUSTER_VAR_KEY = "EMR_CLUSTER_PROJETOS_2INST"
AWS_CONN_ID = "aws_default"

# Resolvido em tempo de execução (não no parse da DAG)
def _aws_region() -> str:
    return Variable.get("aws_region", default_var="sa-east-1")


# DAG filha deve ser disparada pela orquestradora
SCHEDULE = None


def validar_cluster_para_step(**kwargs) -> str:
    """
    Valida se o cluster da variável está apto para receber AddJobFlowSteps.
    Retorna cluster_id no XCom para o EmrAddStepsOperator usar.
    """
    cluster_id = Variable.get(CLUSTER_VAR_KEY, default_var="").strip()
    if not cluster_id:
        raise Exception(f"Variável {CLUSTER_VAR_KEY} vazia.")

    emr = boto3.client("emr", region_name=_aws_region())
    resp = emr.describe_cluster(ClusterId=cluster_id)
    status = resp["Cluster"]["Status"]["State"]

    if status not in ("WAITING", "RUNNING"):
        raise Exception(
            f"Cluster {cluster_id} não apto para AddJobFlowSteps. Estado atual: {status}"
        )

    logger.info("Cluster %s validado com sucesso. Estado=%s", cluster_id, status)
    return cluster_id


def montar_step_metas_2instancia(**kwargs) -> list:
    """
    Monta o step EMR que:
      1. copia o script run_all_metas_emr.py do S3 para /tmp
      2. executa com python3 passando --year <ano da execução>
      3. opcionalmente passa --dry-run
    """
    # Resolvido aqui para evitar Variable.get() no parse da DAG
    script_s3_path = Variable.get(
        "s3_script_metas_2instancia",
        default_var="s3://sgdai-projetos/2instancia/2inst_metacnj/run_all_metas_emr.py",
    )
    dry_run = Variable.get("metacnj_dry_run", default_var="false").strip().lower() in (
        "true",
        "1",
        "yes",
        "y",
    )

    # execution_date é um objeto Python real dentro da função — não usar Jinja aqui
    year = kwargs["execution_date"].year

    comando = (
        f"hadoop fs -copyToLocal -f {script_s3_path} /tmp/run_all_metas_emr.py "
        f"&& python3 /tmp/run_all_metas_emr.py --year {year}"
    )

    if dry_run:
        comando += " --dry-run"

    step = [
        {
            "Name": "2inst_metacnj_full_pipeline",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "bash",
                    "-c",
                    comando,
                ],
            },
        }
    ]

    logger.info("Step montado com sucesso. Dry-run=%s", dry_run)
    logger.info("Comando final do step: %s", comando)
    logger.info("Step final: %s", step)

    return step


default_args = {
    "owner": "adilson.lage",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 4, 19, tz=TZ),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["projeto", "metacnj", "emr", "2inst", "meta"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    validar_cluster = PythonOperator(
        task_id="validar_cluster_para_step",
        python_callable=validar_cluster_para_step,
    )

    montar_step = PythonOperator(
        task_id="montar_step_metas_2instancia",
        python_callable=montar_step_metas_2instancia,
    )

    executar_pipeline_metas = EmrAddStepsOperator(
        task_id="executar_pipeline_metas",
        job_flow_id="{{ ti.xcom_pull(task_ids='validar_cluster_para_step') }}",
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=1200,
        steps="{{ ti.xcom_pull(task_ids='montar_step_metas_2instancia') }}",
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> validar_cluster >> montar_step >> executar_pipeline_metas >> fim
