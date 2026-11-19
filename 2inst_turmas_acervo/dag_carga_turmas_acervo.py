# -*- coding: utf-8 -*-
#
# Criacao - 2026-04-24 - dag_carga_turmas_acervo.py
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
DAG_ID = "dag_carga_turmas_acervo"
TZ = pendulum.timezone("America/Sao_Paulo")
CLUSTER_VAR_KEY = "EMR_CLUSTER_PROJETOS_2INST"
AWS_CONN_ID = "aws_default"


def _aws_region() -> str:
    return Variable.get("aws_region", default_var="sa-east-1")


# DAG filha — disparada pela orquestradora ou manualmente
SCHEDULE = None


def validar_cluster_para_step(**kwargs) -> str:
    """
    Valida se o cluster está apto para receber AddJobFlowSteps.
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

    logger.info("Cluster %s validado. Estado=%s", cluster_id, status)
    return cluster_id


def montar_step_turmas_acervo(**kwargs) -> list:
    """
    Monta o step EMR que:
      1. copia run_all_turmas_acervo_emr.py do S3 para /tmp
      2. executa com python3 passando --dateref <data de execução>
         (run_all chama spark-submit internamente)
      3. opcionalmente passa --dry-run
    """
    script_s3_path = Variable.get(
        "s3_script_turmas_acervo",
        default_var="s3://sgdai-projetos/2instancia/2inst_turmas_acervo/run_all_turmas_acervo_emr.py",
    )
    dry_run = Variable.get(
        "turmas_acervo_dry_run", default_var="false"
    ).strip().lower() in ("true", "1", "yes", "y")

    # execution_date resolvido dentro da função — não usar Jinja aqui
    dateref = kwargs["execution_date"].strftime("%Y-%m-%d")

    comando = (
        f"hadoop fs -copyToLocal -f {script_s3_path} /tmp/run_all_turmas_acervo_emr.py "
        f"&& python3 /tmp/run_all_turmas_acervo_emr.py --dateref {dateref}"
    )

    if dry_run:
        comando += " --dry-run"

    step = [
        {
            "Name": "2inst_turmas_acervo_pipeline",
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

    logger.info("Step montado. dry_run=%s | dateref=%s", dry_run, dateref)
    logger.info("Comando: %s", comando)
    logger.info("Step: %s", step)

    return step


default_args = {
    "owner": "adilson.lage",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 4, 24, tz=TZ),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["projeto", "turmas", "acervo", "emr", "2inst", "iceberg"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    validar_cluster = PythonOperator(
        task_id="validar_cluster_para_step",
        python_callable=validar_cluster_para_step,
    )

    montar_step = PythonOperator(
        task_id="montar_step_turmas_acervo",
        python_callable=montar_step_turmas_acervo,
    )

    executar_pipeline_turmas = EmrAddStepsOperator(
        task_id="executar_pipeline_turmas_acervo",
        job_flow_id="{{ ti.xcom_pull(task_ids='validar_cluster_para_step') }}",
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=1200,
        steps="{{ ti.xcom_pull(task_ids='montar_step_turmas_acervo') }}",
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> validar_cluster >> montar_step >> executar_pipeline_turmas >> fim
