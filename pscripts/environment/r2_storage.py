from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

from pscripts.environment.entities import SourceConfig, SourceValue


class R2SourceValueArchive:
    def __init__(
        self,
        *,
        bucket: str | None,
        endpoint_url: str | None,
        access_key_id: str | None,
        secret_access_key: str | None,
        prefix: str = "environment/source_values",
    ) -> None:
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.prefix = prefix.strip("/")
        self._client = None

    @classmethod
    def from_env(cls) -> "R2SourceValueArchive":
        return cls(
            bucket=os.getenv("R2_BUCKET"),
            endpoint_url=os.getenv("R2_ENDPOINT_URL"),
            access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            prefix=os.getenv("R2_SOURCE_VALUES_PREFIX", "environment/source_values"),
        )

    @property
    def available(self) -> bool:
        return bool(self.bucket and self.endpoint_url and self.access_key_id and self.secret_access_key)

    def missing_settings(self) -> list[str]:
        missing = []
        if not self.bucket:
            missing.append("R2_BUCKET")
        if not self.endpoint_url:
            missing.append("R2_ENDPOINT_URL")
        if not self.access_key_id:
            missing.append("R2_ACCESS_KEY_ID")
        if not self.secret_access_key:
            missing.append("R2_SECRET_ACCESS_KEY")
        return missing

    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name="auto",
            )
        return self._client

    def key_for(self, source: SourceConfig, run_id: str, run_time: datetime) -> str:
        run_date = run_time.strftime("%Y-%m-%d")
        run_hour = run_time.strftime("%H")
        return (
            f"{self.prefix}/run_date={run_date}/run_hour={run_hour}/"
            f"source_code={source.code}/{run_id}.jsonl.gz"
        )

    def write_source_values(
        self,
        *,
        source: SourceConfig,
        run_id: str,
        run_time: datetime,
        values: list[SourceValue],
    ) -> str | None:
        if not self.available or not values:
            return None

        key = self.key_for(source, run_id, run_time)
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            for value in values:
                line = json.dumps(value.to_data2lamer_row(), ensure_ascii=False, separators=(",", ":"))
                gz.write(line.encode("utf-8"))
                gz.write(b"\n")

        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
            Metadata={
                "source-code": source.code,
                "run-id": run_id,
                "run-time": run_time.isoformat(),
                "rows-count": str(len(values)),
            },
        )
        return key

    def list_source_value_keys(
        self,
        *,
        run_time: datetime,
        source_codes: set[str] | None = None,
    ) -> list[str]:
        if not self.available:
            return []

        run_date = run_time.strftime("%Y-%m-%d")
        run_hour = run_time.strftime("%H")
        prefix = f"{self.prefix}/run_date={run_date}/run_hour={run_hour}/"
        keys: list[str] = []
        paginator = self.client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if not key or not key.endswith(".jsonl.gz"):
                    continue
                if source_codes and not any(f"/source_code={code}/" in key for code in source_codes):
                    continue
                keys.append(key)
        return sorted(keys)

    def latest_source_value_keys(
        self,
        *,
        lookback_hours: int = 12,
        source_codes: set[str] | None = None,
        now: datetime | None = None,
    ) -> tuple[datetime | None, list[str]]:
        current = now or datetime.now(timezone.utc)
        current = current.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        for offset in range(lookback_hours + 1):
            run_time = current - timedelta(hours=offset)
            keys = self.list_source_value_keys(run_time=run_time, source_codes=source_codes)
            if keys:
                return run_time, keys
        return None, []

    def read_source_values(self, key: str) -> list[SourceValue]:
        if not self.available:
            return []

        response = self.client().get_object(Bucket=self.bucket, Key=key)
        body = response["Body"].read()
        values: list[SourceValue] = []
        for line in gzip.decompress(body).decode("utf-8").splitlines():
            if line.strip():
                values.append(SourceValue.from_data2lamer_row(json.loads(line)))
        return values


class R2TrainingDatasetArchive:
    def __init__(
        self,
        *,
        bucket: str | None,
        endpoint_url: str | None,
        access_key_id: str | None,
        secret_access_key: str | None,
        prefix: str = "training/dive_visibility",
    ) -> None:
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.prefix = prefix.strip("/")
        self._client = None

    @classmethod
    def from_env(cls) -> "R2TrainingDatasetArchive":
        return cls(
            bucket=os.getenv("R2_BUCKET"),
            endpoint_url=os.getenv("R2_ENDPOINT_URL"),
            access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            prefix=os.getenv("R2_TRAINING_DATASET_PREFIX", "training/dive_visibility"),
        )

    @property
    def available(self) -> bool:
        return bool(self.bucket and self.endpoint_url and self.access_key_id and self.secret_access_key)

    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name="auto",
            )
        return self._client

    def key_for(self, run_time: datetime) -> str:
        run_date = run_time.strftime("%Y-%m-%d")
        run_hour = run_time.strftime("%H")
        return f"{self.prefix}/runs/run_date={run_date}/run_hour={run_hour}/dataset_delta.jsonl.gz"

    def latest_key(self) -> str:
        return f"{self.prefix}/latest.jsonl.gz"

    def _encode_rows(self, rows: list[dict[str, Any]]) -> bytes:
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            for row in rows:
                line = json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str)
                gz.write(line.encode("utf-8"))
                gz.write(b"\n")
        return buffer.getvalue()

    def read_rows(self, key: str) -> list[dict[str, Any]]:
        if not self.available:
            return []

        try:
            response = self.client().get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            error_response = getattr(exc, "response", {}) or {}
            error_code = error_response.get("Error", {}).get("Code") if isinstance(error_response, dict) else None
            if isinstance(exc, KeyError) or error_code in {"NoSuchKey", "404", "NotFound"}:
                return []
            raise

        body = response["Body"].read()
        rows: list[dict[str, Any]] = []
        for line in gzip.decompress(body).decode("utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def read_latest_rows(self) -> list[dict[str, Any]]:
        return self.read_rows(self.latest_key())

    def deduplicate_rows(self, rows: list[dict[str, Any]], *, key_field: str = "outing_id") -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for row in rows:
            row_key = row.get(key_field)
            if row_key is None:
                row_key = "|".join(
                    str(row.get(field) or "")
                    for field in ("spot_id", "sector_id", "observed_at")
                )
            if not row_key:
                continue
            merged[str(row_key)] = row
        return list(merged.values())

    def write_rows(self, *, key: str, rows: list[dict[str, Any]], metadata: dict[str, str]) -> str | None:
        if not self.available or not rows:
            return None

        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=self._encode_rows(rows),
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
            Metadata=metadata,
        )
        return key

    def merge_and_write_rows(
        self,
        *,
        run_time: datetime,
        rows: list[dict[str, Any]],
        key_field: str = "outing_id",
    ) -> dict[str, Any]:
        if not self.available or not rows:
            return {"latest_key": None, "run_key": None, "rows_count": 0, "delta_count": 0}

        incoming_rows = self.deduplicate_rows(rows, key_field=key_field)
        merged_rows = self.deduplicate_rows(
            [*self.read_latest_rows(), *incoming_rows],
            key_field=key_field,
        )
        latest_key = self.latest_key()
        run_key = self.key_for(run_time)

        common_metadata = {
            "dataset": "dive-visibility-training",
            "run-time": run_time.isoformat(),
        }
        self.write_rows(
            key=latest_key,
            rows=merged_rows,
            metadata={**common_metadata, "rows-count": str(len(merged_rows)), "kind": "latest"},
        )
        self.write_rows(
            key=run_key,
            rows=incoming_rows,
            metadata={**common_metadata, "rows-count": str(len(incoming_rows)), "kind": "delta"},
        )
        return {
            "latest_key": latest_key,
            "run_key": run_key,
            "rows_count": len(merged_rows),
            "delta_count": len(incoming_rows),
        }
