from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from postgrest.exceptions import APIError
from postgrest.types import CountMethod, ReturnMethod
from supabase import Client

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.supabase_client import get_data2lamer_supabase, get_vu2lamer_supabase


APP_FORECAST_TABLE = os.environ.get("VU2LAMER_FORECAST_TABLE", "environment_forecasts")
APP_TRAINING_DATASET_VIEW = os.environ.get("VU2LAMER_TRAINING_DATASET_VIEW", "dive_visibility_training_dataset")
SOURCE_VALUES_TABLE = os.environ.get("DATA2LAMER_SOURCE_VALUES_TABLE", "forecast_source_values")
SOURCES_TABLE = os.environ.get("DATA2LAMER_SOURCES_TABLE", "environment_sources")
RUNS_TABLE = os.environ.get("DATA2LAMER_RUNS_TABLE", "environment_sync_runs")
GRID_POINTS_TABLE = os.environ.get("DATA2LAMER_GRID_POINTS_TABLE", "spot_source_grid_points")


def _chunks(rows: list[dict[str, Any]], size: int):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _is_statement_timeout(exc: Exception) -> bool:
    return isinstance(exc, APIError) and getattr(exc, "code", None) == "57014"


class Vu2LamerForecastRepository:
    def __init__(self, client: Client | None = None) -> None:
        self.client = client or get_vu2lamer_supabase()
        self.batch_size = int(os.environ.get("FORECAST_UPSERT_BATCH_SIZE", "100"))

    def delete_expired(self, cutoff: datetime | None = None) -> int:
        if cutoff is None:
            keep_past_hours = int(os.environ.get("FORECAST_KEEP_PAST_HOURS", "48"))
            cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_past_hours)
        cutoff = cutoff.astimezone(timezone.utc)
        resp = (
            self.client.table(APP_FORECAST_TABLE)
            .delete(count=CountMethod.exact, returning=ReturnMethod.minimal)
            .lt("valid_time", cutoff.isoformat())
            .execute()
        )
        return resp.count or 0

    def upsert(self, rows: list[dict[str, Any]]) -> int:
        valid_rows = [row for row in rows if row.get("spot_id") and row.get("valid_time")]
        for batch in _chunks(valid_rows, self.batch_size):
            self._upsert_batch(batch)
        return len(valid_rows)

    def _upsert_batch(self, batch: list[dict[str, Any]]) -> None:
        try:
            (
                self.client.table(APP_FORECAST_TABLE)
                .upsert(
                    batch,
                    on_conflict="spot_id,valid_time",
                    returning=ReturnMethod.minimal,
                )
                .execute()
            )
        except Exception as exc:
            if not _is_statement_timeout(exc) or len(batch) <= 1:
                raise

            midpoint = len(batch) // 2
            print(
                "[WARN] VU2LAMER upsert batch timed out; "
                f"retrying as {midpoint} + {len(batch) - midpoint} rows."
            )
            self._upsert_batch(batch[:midpoint])
            self._upsert_batch(batch[midpoint:])


class Vu2LamerDiveTrainingDatasetRepository:
    def __init__(self, client: Client | None = None) -> None:
        self.client = client or get_vu2lamer_supabase()
        self.batch_size = int(os.environ.get("TRAINING_DATASET_FETCH_BATCH_SIZE", "1000"))

    def fetch_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        start = 0
        while True:
            end = start + self.batch_size - 1
            resp = (
                self.client.table(APP_TRAINING_DATASET_VIEW)
                .select("*")
                .order("observed_at")
                .range(start, end)
                .execute()
            )
            batch = resp.data or []
            rows.extend(batch)
            if len(batch) < self.batch_size:
                break
            start += self.batch_size
        return rows


class Data2LamerForecastRepository:
    def __init__(self, client: Client | None = None) -> None:
        self.client = client or get_data2lamer_supabase()
        self.batch_size = int(os.environ.get("SOURCE_VALUES_UPSERT_BATCH_SIZE", "1000"))
        self.disabled_reason: str | None = None

    @property
    def available(self) -> bool:
        return self.client is not None and self.disabled_reason is None

    def disable(self, exc: Exception) -> None:
        self.disabled_reason = str(exc)
        print(f"[WARN] DATA2LAMER storage disabled for this run: {self.disabled_reason}")

    def ensure_sources(self, sources: list[SourceConfig]) -> None:
        if not self.available:
            return

        rows = [
            {
                "code": source.code,
                "name": source.name,
                "provider": source.provider,
                "kind": source.kind,
                "enabled": source.enabled,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            for source in sources
        ]
        if rows:
            try:
                self.client.table(SOURCES_TABLE).upsert(rows, on_conflict="code").execute()
            except Exception as exc:
                self.disable(exc)

    def create_run(self, source: SourceConfig, run_time: datetime, window_start: datetime, window_end: datetime) -> str:
        run_id = str(uuid.uuid4())
        if not self.available:
            return run_id

        try:
            self.client.table(RUNS_TABLE).insert(
                {
                    "id": run_id,
                    "source_code": source.code,
                    "status": "running",
                    "started_at": run_time.isoformat(),
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "parameters": {
                        "forecast_days": os.environ.get("FORECAST_DAYS", "7"),
                    },
                }
            ).execute()
        except Exception as exc:
            self.disable(exc)
        return run_id

    def finish_run(self, run_id: str, status: str, rows_count: int, error: str | None = None) -> None:
        if not self.available:
            return

        update = {
            "status": status,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "rows_count": rows_count,
            "error": error,
        }
        try:
            self.client.table(RUNS_TABLE).update(update).eq("id", run_id).execute()
        except Exception as exc:
            self.disable(exc)

    def insert_source_values(self, values: list[SourceValue]) -> int:
        if not self.available or os.environ.get("DATA2LAMER_STORE_SOURCE_VALUES", "true").lower() not in {"1", "true", "yes"}:
            return 0

        rows = [value.to_data2lamer_row() for value in values]
        try:
            for batch in _chunks(rows, self.batch_size):
                self.client.table(SOURCE_VALUES_TABLE).insert(batch).execute()
        except Exception as exc:
            self.disable(exc)
            return 0
        return len(rows)

    def upsert_grid_points(self, values: list[SourceValue]) -> int:
        if not self.available:
            return 0

        seen = set()
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()
        for value in values:
            if value.grid_lat is None or value.grid_lon is None:
                continue
            key = (
                value.source_code,
                value.spot_id,
                value.model,
                round(value.grid_lat, 8),
                round(value.grid_lon, 8),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "source_code": value.source_code,
                    "spot_id": value.spot_id,
                    "model": value.model or "default",
                    "grid_lat": value.grid_lat,
                    "grid_lon": value.grid_lon,
                    "last_seen_at": now,
                }
            )

        try:
            for batch in _chunks(rows, self.batch_size):
                (
                    self.client.table(GRID_POINTS_TABLE)
                    .upsert(batch, on_conflict="source_code,spot_id,model,grid_lat,grid_lon")
                    .execute()
                )
        except Exception as exc:
            self.disable(exc)
            return 0
        return len(rows)
