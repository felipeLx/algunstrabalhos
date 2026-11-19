"""
Shared helpers: AWS SSO management, S3 loading, config building.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.yaml"
AWS_SSO_CACHE_DIR = Path.home() / ".aws" / "sso" / "cache"
AWS_CLI_CACHE_DIR = Path.home() / ".aws" / "cli" / "cache"

load_dotenv(ROOT / ".env")


# ─── Environment detection ────────────────────────────────────────────────────

def is_container() -> bool:
    """True when running inside ECS/App Runner container."""
    return bool(
        os.getenv("ECS_CONTAINER_METADATA_URI")
        or os.getenv("ECS_CONTAINER_METADATA_URI_V4")
        or os.getenv("AWS_EXECUTION_ENV")  # set by App Runner
    )


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


def save_config(cfg: dict):
    if is_container():
        return  # read-only filesystem in container — config baked in image
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)


def build_s3_path(bucket: str, prefix: str, projeto: str, tabela: str) -> str:
    parts = [p for p in [bucket, prefix, projeto, tabela] if p]
    return "s3://" + "/".join(parts) + "/"


def resolve_aws_profile(cfg: dict) -> str:
    if is_container():
        return ""  # IAM Task Role / App Runner role — no profile
    return cfg.get("aws_profile", "") or os.getenv("AWS_PROFILE", "")


# ─── AWS SSO ──────────────────────────────────────────────────────────────────

def is_sso_token_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(t in msg for t in [
        "tokenretrievalerror",
        "error when retrieving token from sso",
        "token has expired and refresh failed",
    ])


def run_aws_sso_login(profile: str) -> tuple[bool, str]:
    if is_container():
        return True, "Rodando em container — autenticacao via IAM Role. SSO nao necessario."
    if not profile:
        return False, "Set AWS profile first."
    aws_cli = shutil.which("aws")
    if not aws_cli:
        return False, "AWS CLI not found in PATH. Install AWS CLI v2."
    try:
        result = subprocess.run(
            [aws_cli, "sso", "login", "--profile", profile],
            capture_output=True, text=True, check=False,
        )
    except Exception as exc:
        return False, f"Failed to run AWS SSO login: {exc}"
    output = (result.stdout or "").strip() or (result.stderr or "").strip()
    if result.returncode == 0:
        return True, output or f"AWS SSO login completed for profile '{profile}'."
    return False, output or f"AWS SSO login failed (exit {result.returncode})."


def validate_aws_session(profile: str) -> tuple[bool, str]:
    if is_container():
        return True, "Rodando em container — sessao AWS via IAM Role automatica."
    if not profile:
        return False, "Set AWS profile first."
    aws_cli = shutil.which("aws")
    if not aws_cli:
        return False, "AWS CLI not found in PATH."
    try:
        result = subprocess.run(
            [aws_cli, "sts", "get-caller-identity", "--profile", profile],
            capture_output=True, text=True, check=False,
        )
    except Exception as exc:
        return False, f"Failed to validate AWS session: {exc}"
    output = (result.stdout or "").strip() or (result.stderr or "").strip()
    if result.returncode == 0:
        return True, output or f"AWS session valid for profile '{profile}'."
    return False, output or f"AWS session validation failed (exit {result.returncode})."


def clear_aws_cache() -> tuple[bool, str]:
    removed = 0
    missing = []
    for cache_dir in [AWS_SSO_CACHE_DIR, AWS_CLI_CACHE_DIR]:
        if not cache_dir.exists():
            missing.append(str(cache_dir))
            continue
        for f in cache_dir.glob("*.json"):
            f.unlink(missing_ok=True)
            removed += 1
    if removed:
        return True, f"Removed {removed} AWS cache file(s). Run AWS SSO login again."
    if missing:
        return False, "AWS cache directories not found."
    return False, "No AWS cache files found."


# ─── S3 loading ───────────────────────────────────────────────────────────────

def get_s3_fs(profile: str):
    import s3fs
    if is_container() or not profile:
        return s3fs.S3FileSystem()  # uses IAM role / env credentials
    return s3fs.S3FileSystem(profile=profile)


def is_iceberg_table(path: str, profile: str) -> bool:
    fs = get_s3_fs(profile)
    metadata_path = path.rstrip("/") + "/metadata/"
    try:
        return bool(fs.ls(metadata_path))
    except Exception:
        return False


def _get_boto3_session(profile: str):
    import boto3
    if is_container() or not profile:
        return boto3.Session()
    return boto3.Session(profile_name=profile)


def _get_glue_metadata_location(db: str, table: str, profile: str) -> str:
    """Get current metadata.json location from Glue catalog."""
    session = _get_boto3_session(profile)
    glue = session.client("glue")
    response = glue.get_table(DatabaseName=db, Name=table)
    params = response["Table"].get("Parameters", {})
    meta_loc = params.get("metadata_location", "")
    if not meta_loc:
        raise ValueError(f"No metadata_location in Glue table {db}.{table}. Not an Iceberg table?")
    return meta_loc


def _read_iceberg_current_files(path: str, profile: str, db: str, table: str) -> list[str]:
    """Parse Iceberg metadata to get only data files from current snapshot."""
    import json
    import fastavro

    fs = get_s3_fs(profile)

    # Try Glue catalog first (Glue-managed Iceberg), fall back to version-hint
    meta_file = None
    try:
        meta_file = _get_glue_metadata_location(db, table, profile)
    except Exception:
        meta_dir = path.rstrip("/") + "/metadata/"
        hint_path = meta_dir + "version-hint.text"
        try:
            with fs.open(hint_path, "r") as f:
                version = f.read().strip()
            meta_file = meta_dir + f"v{version}.metadata.json"
        except FileNotFoundError:
            versions = sorted(fs.glob(meta_dir + "v*.metadata.json"))
            if versions:
                meta_file = "s3://" + versions[-1] if not versions[-1].startswith("s3://") else versions[-1]

    if not meta_file:
        return []

    s3_key = meta_file.replace("s3://", "") if meta_file.startswith("s3://") else meta_file
    with fs.open(s3_key, "r") as f:
        metadata = json.loads(f.read())

    snapshot_id = metadata.get("current-snapshot-id")
    if snapshot_id is None or snapshot_id == -1:
        return []

    snapshot = next((s for s in metadata.get("snapshots", []) if s["snapshot-id"] == snapshot_id), None)
    if not snapshot:
        return []

    manifest_list_path = snapshot["manifest-list"]
    ml_key = manifest_list_path.replace("s3://", "") if manifest_list_path.startswith("s3://") else manifest_list_path

    manifest_paths = []
    with fs.open(ml_key, "rb") as f:
        for record in fastavro.reader(f):
            manifest_paths.append(record["manifest_path"])

    added_files = set()
    deleted_files = set()

    for manifest_path in manifest_paths:
        m_key = manifest_path.replace("s3://", "") if manifest_path.startswith("s3://") else manifest_path
        with fs.open(m_key, "rb") as f:
            for record in fastavro.reader(f):
                status = record.get("status", 1)
                data_file = record["data_file"]
                file_path = data_file["file_path"]

                # content: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
                content_type = data_file.get("content", 0)
                if content_type != 0:
                    continue

                if status == 2:  # DELETED
                    deleted_files.add(file_path)
                elif status in (0, 1):  # EXISTING or ADDED
                    added_files.add(file_path)

    return list(added_files - deleted_files)


def load_iceberg_s3(path: str, profile: str, nrows: int, db: str = "", table: str = "") -> pd.DataFrame:
    """Load only current snapshot data files from Iceberg table."""
    fs = get_s3_fs(profile)
    data_files = _read_iceberg_current_files(path, profile, db, table)

    if not data_files:
        raise ValueError(f"No data files found in current Iceberg snapshot at {path}")

    dfs, remaining = [], nrows if nrows > 0 else None
    for f in sorted(data_files, reverse=True):
        s3_key = f.replace("s3://", "") if f.startswith("s3://") else f
        with fs.open(s3_key, "rb") as fh:
            chunk = pd.read_parquet(fh)
        if remaining is not None:
            chunk = chunk.iloc[:remaining]
            remaining -= len(chunk)
            dfs.append(chunk)
            if remaining <= 0:
                break
        else:
            dfs.append(chunk)
    return pd.concat(dfs, ignore_index=True)


def load_parquet_s3(path: str, profile: str, nrows: int) -> pd.DataFrame:
    fs = get_s3_fs(profile)
    files = (
        fs.glob(path.rstrip("/") + "/**/*.parquet")
        or fs.glob(path.rstrip("/") + "/*.parquet")
    )
    if not files:
        files = [path.replace("s3://", "")]
    dfs, remaining = [], nrows if nrows > 0 else None
    for f in sorted(files, reverse=True):
        with fs.open(f, "rb") as fh:
            chunk = pd.read_parquet(fh)
        if remaining is not None:
            chunk = chunk.iloc[:remaining]
            remaining -= len(chunk)
            dfs.append(chunk)
            if remaining <= 0:
                break
        else:
            dfs.append(chunk)
    return pd.concat(dfs, ignore_index=True)
