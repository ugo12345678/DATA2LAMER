from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.timeutils import floor_hour, parse_utc
from pscripts.environment.units import normalize_metric_value


REQUEST_TIMEOUT = 120
MAX_RETRIES = 5
SLEEP_BETWEEN_BATCHES_SEC = 1.0


def _chunk_dataframe(df: pd.DataFrame, chunk_size: int):
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size].copy()


def _get_with_retry(session: requests.Session, url: str, params: dict[str, Any]) -> dict | list:
    last_exc: Exception | None = None
    last_status: int | None = None
    last_body: str | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            last_status = response.status_code
            if response.status_code >= 400:
                last_body = response.text[:500]
            if response.status_code == 429:
                time.sleep(min(30, 2**attempt))
                continue
            if response.status_code == 400:
                response.raise_for_status()
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if last_status == 400:
                break
            if attempt == MAX_RETRIES:
                break
            time.sleep(min(30, 2**attempt))

    reason = f" body={last_body}" if last_body else ""
    raise RuntimeError(f"Open-Meteo request failed for {url} status={last_status}{reason}") from last_exc


def _get_with_variable_fallback(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    variable_map: dict[str, str],
) -> tuple[dict | list, dict[str, str]]:
    try:
        return _get_with_retry(session, url, params), variable_map
    except RuntimeError as exc:
        if "status=400" not in str(exc):
            raise
        print(f"[WARN] Open-Meteo batch failed for {url}; retrying variables one by one: {exc}")

    merged_payload: dict | list | None = None
    valid_variable_map: dict[str, str] = {}

    for raw_variable, metric in variable_map.items():
        single_params = dict(params)
        single_params["hourly"] = raw_variable
        try:
            payload = _get_with_retry(session, url, single_params)
        except RuntimeError as exc:
            print(f"[WARN] Open-Meteo variable skipped for {url}: {raw_variable} ({exc})")
            continue

        if merged_payload is None:
            merged_payload = payload
        else:
            merged_payload = _merge_hourly_payloads(merged_payload, payload)
        valid_variable_map[raw_variable] = metric

    if merged_payload is None:
        raise RuntimeError(f"Open-Meteo source failed for all variables: {url}")

    return merged_payload, valid_variable_map


def _merge_hourly_payloads(left: dict | list, right: dict | list) -> dict | list:
    if isinstance(left, list) and isinstance(right, list):
        for left_item, right_item in zip(left, right):
            _merge_hourly_payload_item(left_item, right_item)
        return left

    if isinstance(left, dict) and isinstance(right, dict):
        _merge_hourly_payload_item(left, right)
        return left

    raise ValueError("Cannot merge different Open-Meteo payload shapes")


def _merge_hourly_payload_item(left: dict, right: dict) -> None:
    left_hourly = left.setdefault("hourly", {})
    right_hourly = right.get("hourly") or {}
    for key, value in right_hourly.items():
        if key == "time":
            left_hourly.setdefault("time", value)
        else:
            left_hourly[key] = value

    left_units = left.setdefault("hourly_units", {})
    for key, value in (right.get("hourly_units") or {}).items():
        left_units[key] = value


def _normalize_payload(payload: dict | list, expected_count: int) -> list[dict]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = [payload]
    else:
        raise ValueError("Unexpected Open-Meteo response format")

    if len(items) != expected_count:
        raise ValueError(f"Unexpected Open-Meteo response count: got={len(items)} expected={expected_count}")

    return items


def _rows_from_hourly_payload(
    *,
    payload_item: dict,
    spot: pd.Series,
    source_code: str,
    variable_map: dict[str, str],
    run_time: datetime,
    resolution_minutes: int,
    model_name: str | None = None,
) -> list[SourceValue]:
    hourly = payload_item.get("hourly") or {}
    times = hourly.get("time")
    if not times:
        return []

    units = payload_item.get("hourly_units") or {}
    grid_lat = payload_item.get("latitude")
    grid_lon = payload_item.get("longitude")
    model = model_name or payload_item.get("model") or source_code
    rows: list[SourceValue] = []

    for idx, raw_time in enumerate(times):
        try:
            valid_time = floor_hour(parse_utc(raw_time))
        except ValueError:
            continue

        for raw_variable, metric in variable_map.items():
            values = hourly.get(raw_variable)
            if values is None or idx >= len(values):
                continue

            normalized, unit = normalize_metric_value(metric, values[idx], units.get(raw_variable))
            if normalized is None:
                continue

            rows.append(
                SourceValue(
                    spot_id=str(spot["spot_id"]),
                    source_code=source_code,
                    valid_time=valid_time,
                    metric=metric,
                    value=normalized,
                    unit=unit,
                    raw_variable=raw_variable,
                    fetched_at=run_time,
                    model=model,
                    resolution_minutes=resolution_minutes,
                    grid_lat=float(grid_lat) if grid_lat is not None else None,
                    grid_lon=float(grid_lon) if grid_lon is not None else None,
                )
            )

    return rows


class OpenMeteoHourlySource(ForecastSource):
    base_url = "https://api.open-meteo.com/v1/forecast"
    forecast_days_cap: int | None = None
    extra_params: dict[str, Any] = {}
    variable_map = {
        "wind_speed_10m": "wind_speed",
        "wind_direction_10m": "wind_direction",
        "wind_gusts_10m": "wind_gusts",
        "temperature_2m": "air_temperature",
        "relative_humidity_2m": "relative_humidity",
        "dew_point_2m": "dew_point",
        "pressure_msl": "pressure_msl",
        "surface_pressure": "surface_pressure",
        "cloud_cover": "cloud_cover",
        "cloud_cover_low": "cloud_cover_low",
        "cloud_cover_mid": "cloud_cover_mid",
        "cloud_cover_high": "cloud_cover_high",
        "precipitation": "precipitation",
        "visibility": "weather_visibility",
    }

    def __init__(self) -> None:
        self.forecast_days = int(os.environ.get("FORECAST_DAYS", "7"))
        self.batch_size = int(os.environ.get("OPEN_METEO_BATCH_SIZE", "20"))
        self.session = requests.Session()

    def request_forecast_days(self) -> int:
        if self.forecast_days_cap is None:
            return self.forecast_days
        return min(self.forecast_days, self.forecast_days_cap)

    def request_forecast_hours(self) -> int:
        return self.request_forecast_days() * 24

    def request_params(self, spots_batch: pd.DataFrame) -> dict[str, Any]:
        params: dict[str, Any] = {
            "latitude": ",".join(spots_batch["lat_center"].astype(float).map(str).tolist()),
            "longitude": ",".join(spots_batch["lon_center"].astype(float).map(str).tolist()),
            "hourly": ",".join(self.variable_map.keys()),
            "forecast_hours": self.request_forecast_hours(),
            "timezone": "UTC",
            "wind_speed_unit": "ms",
            "precipitation_unit": "mm",
        }
        params.update(self.extra_params)
        return params

    def fetch(self, spots: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        rows: list[SourceValue] = []

        for spots_batch in _chunk_dataframe(spots, self.batch_size):
            rows.extend(self._fetch_batch(spots_batch, run_time))
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

        return rows

    def _fetch_batch(self, spots_batch: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        try:
            params = self.request_params(spots_batch)
            payload, variable_map = _get_with_variable_fallback(
                self.session,
                self.base_url,
                params,
                self.variable_map,
            )
            items = _normalize_payload(payload, expected_count=len(spots_batch))
        except RuntimeError as exc:
            if len(spots_batch) <= 1:
                spot = spots_batch.iloc[0]
                print(
                    f"[WARN] {self.config.code} skipped for spot "
                    f"{spot.get('spot_name') or spot.get('spot_id')}: {exc}"
                )
                return []

            midpoint = len(spots_batch) // 2
            print(
                f"[WARN] {self.config.code} batch of {len(spots_batch)} spots failed; "
                "retrying smaller batches."
            )
            return self._fetch_batch(spots_batch.iloc[:midpoint].copy(), run_time) + self._fetch_batch(
                spots_batch.iloc[midpoint:].copy(),
                run_time,
            )

        rows: list[SourceValue] = []
        for (_, spot), item in zip(spots_batch.iterrows(), items):
            rows.extend(
                _rows_from_hourly_payload(
                    payload_item=item,
                    spot=spot,
                    source_code=self.config.code,
                    variable_map=variable_map,
                    run_time=run_time,
                    resolution_minutes=60,
                    model_name=getattr(self, "model_name", self.config.code),
                )
            )
        return rows


class OpenMeteoWeatherSource(OpenMeteoHourlySource):
    config = SourceConfig(
        code="open_meteo_weather",
        name="Open-Meteo Weather Best Match",
        provider="open-meteo",
        kind="weather",
    )
    base_url = "https://api.open-meteo.com/v1/forecast"


class OpenMeteoMeteoFranceSource(OpenMeteoHourlySource):
    config = SourceConfig(
        code="open_meteo_meteofrance",
        name="Open-Meteo Météo-France AROME/ARPEGE",
        provider="open-meteo",
        kind="weather",
    )
    base_url = "https://api.open-meteo.com/v1/meteofrance"
    forecast_days_cap = 4
    variable_map = {
        "wind_speed_10m": "wind_speed",
        "wind_direction_10m": "wind_direction",
        "wind_gusts_10m": "wind_gusts",
        "temperature_2m": "air_temperature",
        "relative_humidity_2m": "relative_humidity",
        "dew_point_2m": "dew_point",
        "pressure_msl": "pressure_msl",
        "surface_pressure": "surface_pressure",
        "cloud_cover": "cloud_cover",
        "cloud_cover_low": "cloud_cover_low",
        "cloud_cover_mid": "cloud_cover_mid",
        "cloud_cover_high": "cloud_cover_high",
        "precipitation": "precipitation",
    }


class OpenMeteoDwdIconSource(OpenMeteoHourlySource):
    config = SourceConfig(
        code="open_meteo_dwd_icon",
        name="Open-Meteo DWD ICON",
        provider="open-meteo",
        kind="weather",
    )
    base_url = "https://api.open-meteo.com/v1/dwd-icon"
    forecast_days_cap = 7


class OpenMeteoGfsSource(OpenMeteoHourlySource):
    config = SourceConfig(
        code="open_meteo_gfs",
        name="Open-Meteo NOAA GFS",
        provider="open-meteo",
        kind="weather",
    )
    base_url = "https://api.open-meteo.com/v1/gfs"
    forecast_days_cap = 7


class OpenMeteoMarineSource(OpenMeteoHourlySource):
    config = SourceConfig(
        code="open_meteo_marine",
        name="Open-Meteo Marine Best Match",
        provider="open-meteo",
        kind="marine",
    )

    base_url = "https://marine-api.open-meteo.com/v1/marine"
    forecast_days_cap = 8
    variable_map = {
        "wave_height": "wave_height",
        "wave_direction": "wave_direction",
        "wave_period": "wave_period",
        "wind_wave_height": "wind_wave_height",
        "wind_wave_direction": "wind_wave_direction",
        "wind_wave_period": "wind_wave_period",
        "swell_wave_height": "swell_wave_height",
        "swell_wave_direction": "swell_wave_direction",
        "swell_wave_period": "swell_wave_period",
        "secondary_swell_wave_height": "secondary_swell_wave_height",
        "secondary_swell_wave_direction": "secondary_swell_wave_direction",
        "secondary_swell_wave_period": "secondary_swell_wave_period",
        "sea_surface_temperature": "water_temperature",
        "sea_level_height_msl": "sea_level_height",
        "ocean_current_velocity": "current_speed",
        "ocean_current_direction": "current_direction",
    }
    extra_params = {
        "cell_selection": "sea",
    }

    def request_params(self, spots_batch: pd.DataFrame) -> dict[str, Any]:
        params: dict[str, Any] = {
            "latitude": ",".join(spots_batch["lat_center"].astype(float).map(str).tolist()),
            "longitude": ",".join(spots_batch["lon_center"].astype(float).map(str).tolist()),
            "hourly": ",".join(self.variable_map.keys()),
            "forecast_hours": self.request_forecast_hours(),
            "timezone": "UTC",
            "length_unit": "metric",
        }
        params.update(self.extra_params)
        return params


class OpenMeteoMarineModelSource(OpenMeteoMarineSource):
    model_name: str

    def request_params(self, spots_batch: pd.DataFrame) -> dict[str, Any]:
        params = super().request_params(spots_batch)
        params["models"] = self.model_name
        return params


class OpenMeteoMarineMeteoFranceWaveSource(OpenMeteoMarineModelSource):
    config = SourceConfig(
        code="open_meteo_marine_meteofrance_wave",
        name="Open-Meteo Marine Météo-France Wave",
        provider="open-meteo",
        kind="marine",
    )
    model_name = "meteofrance_wave"
    forecast_days_cap = 8
    variable_map = {
        "wave_height": "wave_height",
        "wave_direction": "wave_direction",
        "wave_period": "wave_period",
        "wind_wave_height": "wind_wave_height",
        "wind_wave_direction": "wind_wave_direction",
        "wind_wave_period": "wind_wave_period",
        "swell_wave_height": "swell_wave_height",
        "swell_wave_direction": "swell_wave_direction",
        "swell_wave_period": "swell_wave_period",
    }


class OpenMeteoMarineDwdEwamSource(OpenMeteoMarineMeteoFranceWaveSource):
    config = SourceConfig(
        code="open_meteo_marine_dwd_ewam",
        name="Open-Meteo Marine DWD EWAM",
        provider="open-meteo",
        kind="marine",
    )
    model_name = "dwd_ewam"
    forecast_days_cap = 4


class OpenMeteoMarineDwdGwamSource(OpenMeteoMarineMeteoFranceWaveSource):
    config = SourceConfig(
        code="open_meteo_marine_dwd_gwam",
        name="Open-Meteo Marine DWD GWAM",
        provider="open-meteo",
        kind="marine",
    )
    model_name = "dwd_gwam"
    forecast_days_cap = 4


class OpenMeteoMarineGfsWaveSource(OpenMeteoMarineMeteoFranceWaveSource):
    config = SourceConfig(
        code="open_meteo_marine_gfs_wave",
        name="Open-Meteo Marine NOAA GFS Wave",
        provider="open-meteo",
        kind="marine",
    )
    model_name = "ncep_gfswave025"
    forecast_days_cap = 8


class OpenMeteoMarineMeteoFranceCurrentsSource(OpenMeteoMarineModelSource):
    config = SourceConfig(
        code="open_meteo_marine_meteofrance_currents",
        name="Open-Meteo Marine Météo-France Currents",
        provider="open-meteo",
        kind="marine",
    )
    model_name = "meteofrance_currents"
    forecast_days_cap = 8
    variable_map = {
        "ocean_current_velocity": "current_speed",
        "ocean_current_direction": "current_direction",
        "sea_level_height_msl": "sea_level_height",
    }


class OpenMeteoMarineMeteoFranceSstSource(OpenMeteoMarineModelSource):
    config = SourceConfig(
        code="open_meteo_marine_meteofrance_sst",
        name="Open-Meteo Marine Météo-France Sea Surface Temperature",
        provider="open-meteo",
        kind="marine",
    )
    model_name = "meteofrance_sea_surface_temperature"
    forecast_days_cap = 8
    variable_map = {
        "sea_surface_temperature": "water_temperature",
    }
