from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.timeutils import floor_hour, parse_utc
from pscripts.environment.units import normalize_metric_value


REQUEST_TIMEOUT = 60
SLEEP_BETWEEN_REQUESTS_SEC = float(os.environ.get("METNO_SLEEP_BETWEEN_REQUESTS_SEC", "0.25"))


class MetNoLocationForecastSource(ForecastSource):
    config = SourceConfig(
        code="metno_locationforecast",
        name="MET Norway Locationforecast",
        provider="met-norway",
        kind="weather",
    )

    base_url = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
    instant_variable_map = {
        "air_temperature": ("air_temperature", "degC"),
        "relative_humidity": ("relative_humidity", "%"),
        "dew_point_temperature": ("dew_point", "degC"),
        "air_pressure_at_sea_level": ("pressure_msl", "hPa"),
        "cloud_area_fraction": ("cloud_cover", "%"),
        "cloud_area_fraction_low": ("cloud_cover_low", "%"),
        "cloud_area_fraction_medium": ("cloud_cover_mid", "%"),
        "cloud_area_fraction_high": ("cloud_cover_high", "%"),
        "wind_from_direction": ("wind_direction", "deg"),
        "wind_speed": ("wind_speed", "m/s"),
        "wind_speed_of_gust": ("wind_gusts", "m/s"),
    }
    next_hour_variable_map = {
        "precipitation_amount": ("precipitation", "mm"),
    }

    def __init__(self) -> None:
        self.session = requests.Session()
        self.forecast_days = int(os.environ.get("FORECAST_DAYS", "7"))
        self.user_agent = os.environ.get(
            "METNO_USER_AGENT",
            "DATA2LAMER/1.0 https://github.com/ugoma/DATA2LAMER",
        )

    def fetch(self, spots: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        rows: list[SourceValue] = []
        for _, spot in spots.iterrows():
            try:
                rows.extend(self._fetch_spot(spot, run_time))
            except Exception as exc:
                print(f"[WARN] metno_locationforecast failed for spot {spot['spot_id']}: {exc}")
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)
        return rows

    def _fetch_spot(self, spot: pd.Series, run_time: datetime) -> list[SourceValue]:
        params = {
            "lat": f"{float(spot['lat_center']):.5f}",
            "lon": f"{float(spot['lon_center']):.5f}",
        }
        headers = {
            "User-Agent": self.user_agent,
        }
        response = self.session.get(self.base_url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()

        geometry = (payload.get("geometry") or {}).get("coordinates") or []
        grid_lon = float(geometry[0]) if len(geometry) > 0 else None
        grid_lat = float(geometry[1]) if len(geometry) > 1 else None
        model = ",".join((payload.get("properties") or {}).get("meta", {}).get("units", {}).keys()) or None

        rows: list[SourceValue] = []
        timeseries = (payload.get("properties") or {}).get("timeseries") or []
        window_end = run_time + timedelta(days=self.forecast_days)
        for item in timeseries:
            try:
                valid_time = floor_hour(parse_utc(item.get("time")))
            except ValueError:
                continue
            if valid_time < run_time or valid_time >= window_end:
                continue

            data = item.get("data") or {}
            details = (data.get("instant") or {}).get("details") or {}
            rows.extend(
                self._values_from_details(
                    details=details,
                    variable_map=self.instant_variable_map,
                    spot=spot,
                    run_time=run_time,
                    valid_time=valid_time,
                    grid_lat=grid_lat,
                    grid_lon=grid_lon,
                    model=model,
                )
            )

            next_1h_details = (data.get("next_1_hours") or {}).get("details") or {}
            rows.extend(
                self._values_from_details(
                    details=next_1h_details,
                    variable_map=self.next_hour_variable_map,
                    spot=spot,
                    run_time=run_time,
                    valid_time=valid_time,
                    grid_lat=grid_lat,
                    grid_lon=grid_lon,
                    model=model,
                )
            )

        return rows

    def _values_from_details(
        self,
        *,
        details: dict,
        variable_map: dict[str, tuple[str, str]],
        spot: pd.Series,
        run_time: datetime,
        valid_time: datetime,
        grid_lat: float | None,
        grid_lon: float | None,
        model: str | None,
    ) -> list[SourceValue]:
        rows: list[SourceValue] = []
        for raw_variable, (metric, unit_guess) in variable_map.items():
            if raw_variable not in details:
                continue

            normalized, unit = normalize_metric_value(metric, details[raw_variable], unit_guess)
            if normalized is None:
                continue

            rows.append(
                SourceValue(
                    spot_id=str(spot["spot_id"]),
                    source_code=self.config.code,
                    valid_time=valid_time,
                    metric=metric,
                    value=normalized,
                    unit=unit,
                    raw_variable=raw_variable,
                    fetched_at=run_time,
                    model=model,
                    resolution_minutes=60,
                    grid_lat=grid_lat,
                    grid_lon=grid_lon,
                )
            )
        return rows
