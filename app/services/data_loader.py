from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
import streamlit as st


DEFAULT_LOCAL_DATASET_CANDIDATES = [
    "data/serving/dataset_visibility_app.parquet",
    "data/processed/ml/dataset_visibility_mvp_ml_ready_2024_2026.parquet",
]


def _has_r2_secrets() -> bool:
    try:
        return "R2" in st.secrets and bool(st.secrets["R2"].get("bucket"))
    except Exception:
        return False


@st.cache_resource
def get_r2_client():
    if not _has_r2_secrets():
        raise RuntimeError("R2 secrets are not configured.")

    r2 = st.secrets["R2"]
    endpoint_url = r2.get("endpoint_url")
    if not endpoint_url:
        account_id = r2.get("account_id")
        if not account_id:
            raise RuntimeError("Missing R2 endpoint_url or account_id in secrets.")
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=r2["access_key_id"],
        aws_secret_access_key=r2["secret_access_key"],
        region_name=r2.get("region", "auto"),
    )


@st.cache_data(ttl=900, show_spinner=False)
def load_dataset_from_r2() -> pd.DataFrame:
    if not _has_r2_secrets():
        raise RuntimeError("R2 secrets are not configured.")

    client = get_r2_client()
    r2 = st.secrets["R2"]
    bucket = r2["bucket"]
    key = r2.get("dataset_key", "datasets/dataset_visibility_app.parquet")

    obj = client.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    return pd.read_parquet(BytesIO(raw))


def _resolve_local_dataset_path(
    explicit_path: Optional[str] = None,
) -> Path:
    candidates = []

    if explicit_path:
        candidates.append(explicit_path)

    env_path = os.getenv("STREAMLIT_DATASET_PATH")
    if env_path:
        candidates.append(env_path)

    try:
        secret_local_path = st.secrets.get("LOCAL_DATASET_PATH")
        if secret_local_path:
            candidates.append(secret_local_path)
    except Exception:
        pass

    candidates.extend(DEFAULT_LOCAL_DATASET_CANDIDATES)

    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return path

    raise FileNotFoundError(
        "No local dataset found. Checked: "
        + ", ".join(str(Path(c)) for c in candidates)
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_dataset_from_local(local_path: Optional[str] = None) -> pd.DataFrame:
    path = _resolve_local_dataset_path(local_path)
    return pd.read_parquet(path)


def load_dataset(
    source: str = "auto",
    local_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    source:
        - 'auto': try R2 first, fallback to local
        - 'r2': force R2
        - 'local': force local
    """
    source = source.lower().strip()

    if source not in {"auto", "r2", "local"}:
        raise ValueError("source must be one of: auto, r2, local")

    if source == "r2":
        return load_dataset_from_r2()

    if source == "local":
        return load_dataset_from_local(local_path=local_path)

    try:
        return load_dataset_from_r2()
    except Exception:
        return load_dataset_from_local(local_path=local_path)


def get_dataset_source_label(source: str = "auto") -> str:
    source = source.lower().strip()

    if source == "r2":
        return "R2"
    if source == "local":
        return "Local"

    try:
        _ = load_dataset_from_r2()
        return "R2"
    except Exception:
        return "Local"