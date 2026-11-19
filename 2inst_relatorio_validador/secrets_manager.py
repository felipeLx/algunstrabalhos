"""
Credential loader: AWS Secrets Manager in production, env vars locally.

Usage:
    from secrets_manager import get_credentials

    creds = get_credentials()
    # creds["ORACLE_HOST"], creds["ORACLE_PASSWORD"], etc.
"""

import json
import os
from functools import lru_cache


SECRET_NAME = os.getenv("SECRET_NAME", "2inst/relatorio-validador/credentials")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

ENV_KEYS = [
    "ORACLE_HOST",
    "ORACLE_PORT",
    "ORACLE_SERVICE_NAME",
    "ORACLE_USER",
    "ORACLE_PASSWORD",
    "AWS_PROFILE",
]


def is_running_in_container() -> bool:
    return bool(os.getenv("ECS_CONTAINER_METADATA_URI") or os.getenv("ECS_CONTAINER_METADATA_URI_V4"))


def _load_from_secrets_manager() -> dict:
    import boto3
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response["SecretString"])


def _load_from_env() -> dict:
    return {k: os.getenv(k, "") for k in ENV_KEYS}


@lru_cache(maxsize=1)
def get_credentials() -> dict:
    if is_running_in_container():
        try:
            return _load_from_secrets_manager()
        except Exception:
            pass
    return _load_from_env()


def get_oracle_config() -> dict:
    creds = get_credentials()
    return {
        "host": creds.get("ORACLE_HOST", ""),
        "port": int(creds.get("ORACLE_PORT", "1521")),
        "service_name": creds.get("ORACLE_SERVICE_NAME", ""),
        "user": creds.get("ORACLE_USER", ""),
        "password": creds.get("ORACLE_PASSWORD", ""),
    }
