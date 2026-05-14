from __future__ import annotations

import gzip
import json
import os
from datetime import datetime
from io import BytesIO

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
