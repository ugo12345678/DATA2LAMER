from __future__ import annotations

import os
import tempfile
from pathlib import Path

import boto3
import joblib
from botocore.config import Config


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Variable d'environnement manquante: {name}")
    return value.strip()


def normalize_key(key: str) -> str:
    return key.strip().lstrip("/")


def load_model_from_r2():
    endpoint_url = get_required_env("R2_ENDPOINT_URL")
    access_key_id = get_required_env("R2_ACCESS_KEY_ID")
    secret_access_key = get_required_env("R2_SECRET_ACCESS_KEY")
    bucket = get_required_env("R2_BUCKET")
    model_key = normalize_key(get_required_env("R2_MODEL_KEY"))

    print(f"R2 endpoint: {endpoint_url}")
    print(f"R2 bucket: {bucket}")
    print(f"R2 model key: {model_key}")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )

    tmp_path: Path | None = None
    try:
        response = client.get_object(Bucket=bucket, Key=model_key)

        with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(response["Body"].read())

        model = joblib.load(tmp_path)
        print("Model chargé depuis R2 avec succès.")
        return model

    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)