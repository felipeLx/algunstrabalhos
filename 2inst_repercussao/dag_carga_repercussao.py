# -*- coding: utf-8 -*-
#
# Criacao - 2026-05-06 - dag_carga_repercussao.py
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
DAG_ID = "dag_carga_repercussao"
TZ = pendulum.timezone("America/Sao_Paulo")
CLUSTER_VAR_KEY = "EMR_CLUSTER_PROJETOS_2INST"
AWS_CONN_ID = "aws_default"

ICEBERG_JAR = "/usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar"

# DAG filha deve ser disparada pela orquestradora
SCHEDULE = None


def _aws_region() -> str:
    return Variable.get("aws_region", default_var="sa-east-1")


def validar_cluster_para_step(**kwargs) -> str:
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


def montar_steps_repercussao(**kwargs) -> list:
    """
    Monta dois steps EMR sequenciais:
      1. ejud_repercussao_emr.py  (ind_sistema=1)
      2. eproc_repercussao_emr.py (ind_sistema=2)
    Ambos recebem --year <ano> --month <mes> da execution_date.
    """
    ejud_script = Variable.get(
        "s3_script_ejud_repercussao",
        default_var="s3://sgdai-projetos/2instancia/2inst_repercussao/ejud/scripts/ejud_repercussao_emr.py",
    )
    eproc_script = Variable.get(
        "s3_script_eproc_repercussao",
        default_var="s3://sgdai-projetos/2instancia/2inst_repercussao/eproc2g/scripts/eproc_repercussao_emr.py",
    )

    execution_date = kwargs["execution_date"]
    year = execution_date.year
    month = execution_date.month

    def _spark_submit_cmd(script_s3: str) -> str:
        return (
            f"spark-submit --master yarn --deploy-mode client "
            f"--jars {ICEBERG_JAR} "
            f"{script_s3} "
            f"--year {year} --month {month}"
        )

    steps = [
        {
            "Name": "2inst_repercussao_ejud",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["bash", "-c", _spark_submit_cmd(ejud_script)],
            },
        },
        {
            "Name": "2inst_repercussao_eproc",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["bash", "-c", _spark_submit_cmd(eproc_script)],
            },
        },
    ]

    logger.info("Steps montados | year=%d month=%d", year, month)
    for s in steps:
        logger.info("Step: %s | cmd: %s", s["Name"], s["HadoopJarStep"]["Args"][2])

    return steps


default_args = {
    "owner": "adilson.lage",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 5, 6, tz=TZ),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["projeto", "repercussao", "emr", "2inst"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    validar_cluster = PythonOperator(
        task_id="validar_cluster_para_step",
        python_callable=validar_cluster_para_step,
    )

    montar_steps = PythonOperator(
        task_id="montar_steps_repercussao",
        python_callable=montar_steps_repercussao,
    )

    executar_pipeline_repercussao = EmrAddStepsOperator(
        task_id="executar_pipeline_repercussao",
        job_flow_id="{{ ti.xcom_pull(task_ids='validar_cluster_para_step') }}",
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=1200,
        steps="{{ ti.xcom_pull(task_ids='montar_steps_repercussao') }}",
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> validar_cluster >> montar_steps >> executar_pipeline_repercussao >> fim
