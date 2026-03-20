from __future__ import annotations

import os
import tempfile
from pathlib import Path

import boto3
import joblib


def load_model_from_r2():
    endpoint_url = os.environ["R2_ENDPOINT_URL"]
    access_key_id = os.environ["R2_ACCESS_KEY_ID"]
    secret_access_key = os.environ["R2_SECRET_ACCESS_KEY"]
    bucket = os.environ["R2_BUCKET"]
    model_key = os.environ["R2_MODEL_KEY"]

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        client.download_file(bucket, model_key, str(tmp_path))
        model = joblib.load(tmp_path)
        return model
    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)