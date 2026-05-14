from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from supabase import Client

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.supabase_client import get_data2lamer_supabase, get_vu2lamer_supabase


APP_FORECAST_TABLE = os.environ.get("VU2LAMER_FORECAST_TABLE", "environment_forecasts")
SOURCE_VALUES_TABLE = os.environ.get("DATA2LAMER_SOURCE_VALUES_TABLE", "forecast_source_values")
SOURCES_TABLE = os.environ.get("DATA2LAMER_SOURCES_TABLE", "environment_sources")
RUNS_TABLE = os.environ.get("DATA2LAMER_RUNS_TABLE", "environment_sync_runs")
GRID_POINTS_TABLE = os.environ.get("DATA2LAMER_GRID_POINTS_TABLE", "spot_source_grid_points")


def _chunks(rows: list[dict[str, Any]], size: int):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


class Vu2LamerForecastRepository:
    def __init__(self, client: Client | None = None) -> None:
        self.client = client or get_vu2lamer_supabase()
        self.batch_size = int(os.environ.get("FORECAST_UPSERT_BATCH_SIZE", "500"))

    def delete_expired(self) -> int:
        keep_past_hours = int(os.environ.get("FORECAST_KEEP_PAST_HOURS", "48"))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_past_hours)
        resp = (
            self.client.table(APP_FORECAST_TABLE)
            .delete()
            .lt("valid_time", cutoff.isoformat())
            .execute()
        )
        return len(resp.data or [])

    def upsert(self, rows: list[dict[str, Any]]) -> int:
        valid_rows = [row for row in rows if row.get("spot_id") and row.get("valid_time")]
        for batch in _chunks(valid_rows, self.batch_size):
            (
                self.client.table(APP_FORECAST_TABLE)
                .upsert(batch, on_conflict="spot_id,valid_time")
                .execute()
            )
        return len(valid_rows)


class Data2LamerForecastRepository:
    def __init__(self, client: Client | None = None) -> None:
        self.client = client or get_data2lamer_supabase()
        self.batch_size = int(os.environ.get("SOURCE_VALUES_UPSERT_BATCH_SIZE", "1000"))

    @property
    def available(self) -> bool:
        return self.client is not None

    def ensure_sources(self, sources: list[SourceConfig]) -> None:
        if not self.client:
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
            self.client.table(SOURCES_TABLE).upsert(rows, on_conflict="code").execute()

    def create_run(self, source: SourceConfig, run_time: datetime, window_start: datetime, window_end: datetime) -> str:
        run_id = str(uuid.uuid4())
        if not self.client:
            return run_id

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
        return run_id

    def finish_run(self, run_id: str, status: str, rows_count: int, error: str | None = None) -> None:
        if not self.client:
            return

        update = {
            "status": status,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "rows_count": rows_count,
            "error": error,
        }
        self.client.table(RUNS_TABLE).update(update).eq("id", run_id).execute()

    def insert_source_values(self, values: list[SourceValue]) -> int:
        if not self.client or os.environ.get("DATA2LAMER_STORE_SOURCE_VALUES", "true").lower() not in {"1", "true", "yes"}:
            return 0

        rows = [value.to_data2lamer_row() for value in values]
        for batch in _chunks(rows, self.batch_size):
            self.client.table(SOURCE_VALUES_TABLE).insert(batch).execute()
        return len(rows)

    def upsert_grid_points(self, values: list[SourceValue]) -> int:
        if not self.client:
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

        for batch in _chunks(rows, self.batch_size):
            (
                self.client.table(GRID_POINTS_TABLE)
                .upsert(batch, on_conflict="source_code,spot_id,model,grid_lat,grid_lon")
                .execute()
            )
        return len(rows)
