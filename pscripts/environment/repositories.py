from __future__ import annotations

import os
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from postgrest.exceptions import APIError
from postgrest.types import ReturnMethod
from supabase import Client

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.supabase_client import get_data2lamer_supabase, get_vu2lamer_supabase


APP_FORECAST_TABLE = os.environ.get("VU2LAMER_FORECAST_TABLE", "environment_forecasts")
APP_TRAINING_DATASET_VIEW = os.environ.get("VU2LAMER_TRAINING_DATASET_VIEW", "dive_visibility_training_dataset")
APP_DIVES_TABLE = os.environ.get("VU2LAMER_DIVES_TABLE", "dives")
APP_DIVE_SPOTS_TABLE = os.environ.get("VU2LAMER_DIVE_SPOTS_TABLE", "dive_spots")
APP_DIVE_SPOT_IMAGES_TABLE = os.environ.get("VU2LAMER_DIVE_SPOT_IMAGES_TABLE", "dive_spot_images")
APP_SPOTS_TABLE = os.environ.get("VU2LAMER_SPOTS_TABLE", os.environ.get("SPOTS_TABLE", "spots"))
SOURCE_VALUES_TABLE = os.environ.get("DATA2LAMER_SOURCE_VALUES_TABLE", "forecast_source_values")
SOURCES_TABLE = os.environ.get("DATA2LAMER_SOURCES_TABLE", "environment_sources")
RUNS_TABLE = os.environ.get("DATA2LAMER_RUNS_TABLE", "environment_sync_runs")
GRID_POINTS_TABLE = os.environ.get("DATA2LAMER_GRID_POINTS_TABLE", "spot_source_grid_points")

FORECAST_DATASET_COLUMNS = [
    "valid_time",
    "forecast_run_at",
    "forecast_horizon_hours",
    "sources",
    "provenance",
    "wind_speed_ms",
    "wind_gusts_ms",
    "wind_direction_deg",
    "air_temperature_c",
    "relative_humidity_pct",
    "dew_point_c",
    "pressure_msl_hpa",
    "surface_pressure_hpa",
    "cloud_cover_pct",
    "cloud_cover_low_pct",
    "cloud_cover_mid_pct",
    "cloud_cover_high_pct",
    "precipitation_mm",
    "weather_visibility_m",
    "wave_height_m",
    "wave_period_s",
    "wave_direction_deg",
    "wind_wave_height_m",
    "wind_wave_period_s",
    "wind_wave_direction_deg",
    "swell_wave_height_m",
    "swell_wave_period_s",
    "swell_wave_direction_deg",
    "secondary_swell_wave_height_m",
    "secondary_swell_wave_period_s",
    "secondary_swell_wave_direction_deg",
    "water_temperature_c",
    "sea_level_height_m",
    "tide_coefficient",
    "current_speed_ms",
    "current_direction_deg",
    "salinity_psu",
    "chlorophyll_mg_m3",
    "phytoplankton_carbon_mmol_m3",
    "net_primary_production_mg_m3_day",
    "euphotic_depth_m",
    "algal_bloom_risk",
    "light_attenuation_m1",
]


def _chunks(rows: list[dict[str, Any]], size: int):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _is_statement_timeout(exc: Exception) -> bool:
    return isinstance(exc, APIError) and getattr(exc, "code", None) == "57014"


def _is_missing_postgrest_relation(exc: Exception) -> bool:
    return isinstance(exc, APIError) and getattr(exc, "code", None) == "PGRST205"


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("Z", "+00:00")
        if len(text) >= 3 and text[-3] in {"+", "-"} and text[-2:].isdigit():
            text = f"{text}:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_hour(value: Any) -> datetime | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return parsed.replace(minute=0, second=0, microsecond=0)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _spot_center(row: dict[str, Any], min_column: str, max_column: str) -> float | None:
    minimum = _as_float(row.get(min_column))
    maximum = _as_float(row.get(max_column))
    if minimum is None or maximum is None:
        return None
    return (minimum + maximum) / 2.0


class Vu2LamerForecastRepository:
    def __init__(self, client: Client | None = None) -> None:
        self.client = client or get_vu2lamer_supabase()
        self.batch_size = int(os.environ.get("FORECAST_UPSERT_BATCH_SIZE", "100"))
        self.delete_batch_size = int(os.environ.get("FORECAST_DELETE_BATCH_SIZE", "100"))

    def delete_expired(self, cutoff: datetime | None = None) -> int:
        if cutoff is None:
            keep_past_hours = int(os.environ.get("FORECAST_KEEP_PAST_HOURS", "48"))
            cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_past_hours)
        cutoff = cutoff.astimezone(timezone.utc)
        deleted = 0
        while True:
            expired_ids = self._expired_row_ids(cutoff)
            if not expired_ids:
                break
            deleted += self._delete_ids(expired_ids)
        return deleted

    def _expired_row_ids(self, cutoff: datetime) -> list[str]:
        resp = (
            self.client.table(APP_FORECAST_TABLE)
            .select("id")
            .lt("valid_time", cutoff.isoformat())
            .order("valid_time")
            .limit(self.delete_batch_size)
            .execute()
        )
        return [str(row["id"]) for row in (resp.data or []) if row.get("id")]

    def _delete_ids(self, ids: list[str]) -> int:
        if not ids:
            return 0
        try:
            (
                self.client.table(APP_FORECAST_TABLE)
                .delete(returning=ReturnMethod.minimal)
                .in_("id", ids)
                .execute()
            )
        except Exception as exc:
            if not _is_statement_timeout(exc) or len(ids) <= 1:
                raise
            midpoint = len(ids) // 2
            print(
                "[WARN] VU2LAMER delete batch timed out; "
                f"retrying as {midpoint} + {len(ids) - midpoint} rows."
            )
            return self._delete_ids(ids[:midpoint]) + self._delete_ids(ids[midpoint:])
        return len(ids)

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
        self.filter_batch_size = int(os.environ.get("TRAINING_DATASET_FILTER_BATCH_SIZE", "100"))

    def fetch_rows(self) -> list[dict[str, Any]]:
        source = os.environ.get("TRAINING_DATASET_SOURCE", "app_tables").lower()
        if source == "view":
            return self._fetch_view_rows()
        if source not in {"app_tables", "direct"}:
            raise ValueError("TRAINING_DATASET_SOURCE must be 'app_tables', 'direct', or 'view'.")

        try:
            return self._fetch_app_table_rows()
        except Exception as exc:
            if not _is_missing_postgrest_relation(exc):
                raise
            if os.environ.get("TRAINING_DATASET_FALLBACK_TO_VIEW", "true").lower() not in {"1", "true", "yes"}:
                raise
            print(f"[WARN] VU2LAMER app-table dataset read unavailable, trying legacy view: {exc}")
            return self._fetch_view_rows()

    def _fetch_view_rows(self) -> list[dict[str, Any]]:
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

    def _fetch_rows(self, table_name: str, select_columns: str, *, query_builder=None, order: str | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        start = 0
        while True:
            end = start + self.batch_size - 1
            query = self.client.table(table_name).select(select_columns)
            if query_builder is not None:
                query = query_builder(query)
            if order is not None:
                query = query.order(order)
            resp = query.range(start, end).execute()
            batch = resp.data or []
            rows.extend(batch)
            if len(batch) < self.batch_size:
                break
            start += self.batch_size
        return rows

    def _fetch_rows_by_values(self, table_name: str, select_columns: str, column: str, values: set[str], *, order: str | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        clean_values = sorted(value for value in values if value)
        for batch in _chunks(clean_values, self.filter_batch_size):
            rows.extend(
                self._fetch_rows(
                    table_name,
                    select_columns,
                    query_builder=lambda query, batch=batch: query.in_(column, batch),
                    order=order,
                )
            )
        return rows

    def _fetch_app_table_rows(self) -> list[dict[str, Any]]:
        observations = self._fetch_rows(
            APP_DIVE_SPOTS_TABLE,
            "id,dive_id,spot_id,estimated_visibility,visited_at,latitude,longitude,label,comment,position,created_at",
            order="visited_at",
        )
        observations = [
            row
            for row in observations
            if row.get("id") and row.get("dive_id") and row.get("estimated_visibility") is not None and row.get("visited_at")
        ]
        if not observations:
            return []

        dive_ids = {str(row["dive_id"]) for row in observations}
        dives = {
            str(row["id"]): row
            for row in self._fetch_rows_by_values(
                APP_DIVES_TABLE,
                "id,spot_id,dive_date,created_at,updated_at,cover_image_url,cover_image_path,dive_type,club_publication_status",
                "id",
                dive_ids,
            )
        }
        images_by_dive_spot = self._images_by_dive_spot({str(row["id"]) for row in observations})

        spot_ids: set[str] = set()
        prepared_observations: list[tuple[dict[str, Any], dict[str, Any], str, datetime]] = []
        for observation in observations:
            dive = dives.get(str(observation["dive_id"]), {})
            spot_id = str(observation.get("spot_id") or dive.get("spot_id") or "")
            observed_hour = _utc_hour(observation.get("visited_at"))
            if not spot_id or observed_hour is None:
                continue
            spot_ids.add(spot_id)
            prepared_observations.append((observation, dive, spot_id, observed_hour))

        if not prepared_observations:
            return []

        spots = {
            str(row["id"]): row
            for row in self._fetch_rows_by_values(
                APP_SPOTS_TABLE,
                "id,name,latitude_min,latitude_max,longitude_min,longitude_max",
                "id",
                spot_ids,
            )
        }
        forecasts = self._forecast_rows_by_spot_hour(prepared_observations)

        dataset_rows: list[dict[str, Any]] = []
        for observation, dive, spot_id, observed_hour in prepared_observations:
            forecast = forecasts.get((spot_id, observed_hour.isoformat()))
            if not forecast:
                continue

            spot = spots.get(spot_id, {})
            latitude = _as_float(observation.get("latitude"))
            if latitude is None:
                latitude = _spot_center(spot, "latitude_min", "latitude_max")
            longitude = _as_float(observation.get("longitude"))
            if longitude is None:
                longitude = _spot_center(spot, "longitude_min", "longitude_max")
            visibility = _as_float(observation.get("estimated_visibility"))
            observed_at = _parse_datetime(observation.get("visited_at"))
            if latitude is None or longitude is None or visibility is None or observed_at is None:
                continue

            dive_id = str(observation["dive_id"])
            image = images_by_dive_spot.get(str(observation["id"])) or dive.get("cover_image_url")
            updated_at = _parse_datetime(dive.get("updated_at") or observation.get("created_at"))
            row = {
                "outing_id": str(observation["id"]),
                "dive_id": dive_id,
                "dive_spot_id": str(observation["id"]),
                "spot_id": spot_id,
                "spot_label": observation.get("label") or spot.get("name"),
                "sector_id": None,
                "longitude": longitude,
                "latitude": latitude,
                "observed_at": observed_at.isoformat(),
                "outing_updated_at": updated_at.isoformat() if updated_at else None,
                "observed_visibility_m": visibility,
                "visibility_image_url": image,
            }
            for column in FORECAST_DATASET_COLUMNS:
                row[column] = forecast.get(column)
            dataset_rows.append(row)

        dataset_rows.sort(key=lambda row: (str(row.get("observed_at") or ""), str(row.get("outing_id") or "")))
        return dataset_rows

    def _images_by_dive_spot(self, dive_spot_ids: set[str]) -> dict[str, str]:
        rows = self._fetch_rows_by_values(
            APP_DIVE_SPOT_IMAGES_TABLE,
            "dive_spot_id,image_url,image_path,position,created_at,use_for_visibility",
            "dive_spot_id",
            dive_spot_ids,
        )
        rows.sort(
            key=lambda row: (
                str(row.get("dive_spot_id") or ""),
                not bool(row.get("use_for_visibility")),
                int(row.get("position") or 0),
                str(row.get("created_at") or ""),
            )
        )
        images: dict[str, str] = {}
        for row in rows:
            dive_spot_id = str(row.get("dive_spot_id") or "")
            image = row.get("image_url") or row.get("image_path")
            if dive_spot_id and image and dive_spot_id not in images:
                images[dive_spot_id] = str(image)
        return images

    def _forecast_rows_by_spot_hour(
        self,
        observations: list[tuple[dict[str, Any], dict[str, Any], str, datetime]],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        hours_by_spot: dict[str, list[datetime]] = defaultdict(list)
        for _, _, spot_id, observed_hour in observations:
            hours_by_spot[spot_id].append(observed_hour)

        select_columns = ",".join(["spot_id", *FORECAST_DATASET_COLUMNS])
        forecasts: dict[tuple[str, str], dict[str, Any]] = {}
        for spot_id, hours in hours_by_spot.items():
            start = min(hours)
            end = max(hours)
            rows = self._fetch_rows(
                APP_FORECAST_TABLE,
                select_columns,
                query_builder=lambda query, spot_id=spot_id, start=start, end=end: (
                    query.eq("spot_id", spot_id)
                    .gte("valid_time", start.isoformat())
                    .lte("valid_time", end.isoformat())
                ),
                order="valid_time",
            )
            for row in rows:
                valid_hour = _utc_hour(row.get("valid_time"))
                if valid_hour is None:
                    continue
                forecasts[(str(row.get("spot_id")), valid_hour.isoformat())] = row
        return forecasts


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
