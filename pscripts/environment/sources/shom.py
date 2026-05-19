from __future__ import annotations

import json
import os
import re
from calendar import isleap
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.units import normalize_metric_value


REQUEST_TIMEOUT = 60


def _coefficient_from_value(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and not value.is_integer():
        return None

    try:
        coefficient = int(str(value).strip())
    except (TypeError, ValueError):
        return None

    if 20 <= coefficient <= 120:
        return coefficient
    return None


def _coefficients_from_value(value: Any) -> list[int]:
    if isinstance(value, list):
        coefficients: list[int] = []
        for item in value:
            coefficients.extend(_coefficients_from_value(item))
        return coefficients

    coefficient = _coefficient_from_value(value)
    return [coefficient] if coefficient is not None else []


def _date_from_value(value: Any) -> date | None:
    if not isinstance(value, str):
        return None

    match = re.match(r"^(\d{4}-\d{2}-\d{2})", value.strip())
    if not match:
        return None

    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def _is_date_key(value: str) -> bool:
    return _date_from_value(value) is not None


def _looks_like_date_field(key: str) -> bool:
    return key.lower() in {
        "date",
        "datetime",
        "time",
        "timestamp",
        "valid_time",
        "utc",
        "t",
    }


def _looks_like_coefficient_field(key: str) -> bool:
    normalized = key.lower()
    return normalized in {
        "coefficient",
        "coefficients",
        "coeff",
        "coeffs",
        "coef",
        "tidal_coefficient",
        "tide_coefficient",
    }


def _merge_coefficients(target: dict[date, list[int]], current_date: date, values: list[int]) -> None:
    if values:
        target.setdefault(current_date, []).extend(values)


def _walk_coefficients(payload: Any, target: dict[date, list[int]]) -> None:
    if isinstance(payload, list):
        for item in payload:
            _walk_coefficients(item, target)
        return

    if not isinstance(payload, dict):
        return

    for key, value in payload.items():
        if _is_date_key(str(key)):
            values = _coefficients_from_value(value)
            if values:
                _merge_coefficients(target, date.fromisoformat(str(key)[:10]), values)

    current_date: date | None = None
    values: list[int] = []
    for key, value in payload.items():
        if _looks_like_date_field(str(key)):
            current_date = _date_from_value(value) or current_date
        if _looks_like_coefficient_field(str(key)):
            values.extend(_coefficients_from_value(value))

    if current_date is not None:
        _merge_coefficients(target, current_date, values)

    for value in payload.values():
        if isinstance(value, (dict, list)):
            _walk_coefficients(value, target)


def _parse_matrix_coefficients(payload: Any, start_date: date) -> dict[date, list[int]]:
    if not isinstance(payload, list):
        return {}

    coefficients_by_date: dict[date, list[int]] = {}
    for month_offset, month_values in enumerate(payload):
        if not isinstance(month_values, list):
            continue
        month_index = start_date.month - 1 + month_offset
        year = start_date.year + month_index // 12
        month = month_index % 12 + 1

        for day, day_values in enumerate(month_values, start=1):
            try:
                current_date = date(year, month, day)
            except ValueError:
                continue
            _merge_coefficients(coefficients_by_date, current_date, _coefficients_from_value(day_values))

    return coefficients_by_date


def parse_shom_coefficients(payload: Any, start_date: date | None = None) -> dict[date, list[int]]:
    if isinstance(payload, str):
        payload = json.loads(payload)

    if start_date is not None:
        matrix_values = _parse_matrix_coefficients(payload, start_date)
        if matrix_values:
            return matrix_values

    coefficients_by_date: dict[date, list[int]] = {}
    _walk_coefficients(payload, coefficients_by_date)
    return coefficients_by_date


def _hourly_times(start: datetime, end: datetime):
    current = start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    while current < end:
        yield current
        current += timedelta(hours=1)


class ShomTideCoefficientSource(ForecastSource):
    config = SourceConfig(
        code="shom_tide_coefficients",
        name="SHOM Tide Coefficients",
        provider="shom",
        kind="tide",
    )

    def __init__(self) -> None:
        self.session = requests.Session()
        self.forecast_days = int(os.environ.get("FORECAST_DAYS", "7"))
        self.base_url = os.environ.get(
            "SHOM_TIDE_BASE_URL",
            "https://services.data.shom.fr/b2q8lrcdl4s04cbabsj4nhcb/hdm",
        )
        self.harbor = os.environ.get("SHOM_TIDE_HARBOR", "BREST")
        self.utc_mode = os.environ.get("SHOM_TIDE_UTC", "0")
        self.target_tz = ZoneInfo(os.environ.get("FORECAST_TARGET_TIMEZONE", "Europe/Paris"))
        self.daily_reducer = os.environ.get("TIDE_COEFFICIENT_DAILY_REDUCER", "max").lower()

    def fetch(self, spots: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        coefficients_by_date, source_url = self._fetch_coefficients(run_time)
        window_end = run_time + timedelta(days=self.forecast_days)

        rows: list[SourceValue] = []
        for valid_time in _hourly_times(run_time, window_end):
            local_date = valid_time.astimezone(self.target_tz).date()
            coefficients = coefficients_by_date.get(local_date)
            if not coefficients:
                continue

            coefficient_value = self._daily_value(coefficients)
            normalized, unit = normalize_metric_value("tide_coefficient", coefficient_value, "coef")
            if normalized is None:
                continue

            for _, spot in spots.iterrows():
                rows.append(
                    SourceValue(
                        spot_id=str(spot["spot_id"]),
                        source_code=self.config.code,
                        valid_time=valid_time,
                        metric="tide_coefficient",
                        value=normalized,
                        unit=unit,
                        raw_variable=f"daily_tide_coefficient_{self.daily_reducer}",
                        fetched_at=run_time,
                        model=f"harbor={self.harbor}",
                        resolution_minutes=1440,
                        quality_flags={
                            "daily_reducer": self.daily_reducer,
                            "reference_harbor": self.harbor,
                            "source_url": source_url,
                        },
                    )
                )

        return rows

    def _fetch_coefficients(self, run_time: datetime) -> tuple[dict[date, list[int]], str]:
        window_start = run_time.astimezone(self.target_tz).date()
        window_end = (run_time + timedelta(days=self.forecast_days)).astimezone(self.target_tz).date()
        years = range(window_start.year, window_end.year + 1)

        coefficients_by_date: dict[date, list[int]] = {}
        source_urls: list[str] = []
        for year in years:
            year_start = date(year, 1, 1)
            response = self.session.get(
                self._url(),
                params=self._params(year_start),
                headers={"Accept": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            try:
                payload = response.json()
            except ValueError:
                payload = response.text

            for current_date, coefficients in parse_shom_coefficients(payload, year_start).items():
                coefficients_by_date[current_date] = coefficients
            source_urls.append(getattr(response, "url", self._url()))

        return coefficients_by_date, ",".join(source_urls)

    def _params(self, start_date: date) -> dict[str, Any]:
        return {
            "harborName": self.harbor,
            "date": start_date.isoformat(),
            "duration": 366 if isleap(start_date.year) else 365,
            "utc": self.utc_mode,
            "correlation": "1",
        }

    def _daily_value(self, coefficients: list[int]) -> float:
        if self.daily_reducer == "mean":
            return sum(coefficients) / len(coefficients)
        if self.daily_reducer == "min":
            return float(min(coefficients))
        return float(max(coefficients))

    def _url(self) -> str:
        return f"{self.base_url.rstrip('/')}/spm/coeff"


def shom_tide_enabled() -> bool:
    return os.environ.get("ENABLE_SHOM_TIDES", "true").lower() in {"1", "true", "yes"}
