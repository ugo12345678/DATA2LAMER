from __future__ import annotations

import math
import os
from collections import defaultdict
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from pscripts.environment.entities import SourceValue
from pscripts.environment.metrics import METRICS
from pscripts.environment.timeutils import horizon_hours


DERIVED_TIDE_SOURCE_CODE = "derived_tide_range"
TIDE_APPROX_COEFFICIENT_RANGE_100_M = 6.10


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _circular_mean_deg(values: list[float]) -> float | None:
    if not values:
        return None
    radians = [math.radians(value % 360.0) for value in values]
    sin_mean = sum(math.sin(value) for value in radians) / len(radians)
    cos_mean = sum(math.cos(value) for value in radians) / len(radians)
    if sin_mean == 0 and cos_mean == 0:
        return None
    return (math.degrees(math.atan2(sin_mean, cos_mean)) + 360.0) % 360.0


def _source_trace(value: SourceValue) -> dict[str, Any]:
    return {
        "source": value.source_code,
        "value": value.value,
        "unit": value.unit,
        "raw_variable": value.raw_variable,
        "model": value.model,
        "resolution_minutes": value.resolution_minutes,
        "grid_lat": value.grid_lat,
        "grid_lon": value.grid_lon,
        "run_id": value.run_id,
    }


def _target_date(value: SourceValue, target_tz: ZoneInfo) -> str:
    return value.valid_time.astimezone(target_tz).date().isoformat()


def derive_tide_coefficient_from_range(high_water_m: float, low_water_m: float, range_unit_m: float) -> float | None:
    if range_unit_m <= 0:
        return None

    coefficient = (high_water_m - low_water_m) / range_unit_m * 100.0
    if not 20.0 <= coefficient <= 120.0:
        return None
    return coefficient


def _derive_tide_coefficients(values: list[SourceValue], run_time: datetime, target_tz: ZoneInfo) -> list[SourceValue]:
    if os.environ.get("ENABLE_DERIVED_TIDE_COEFFICIENTS", "true").lower() not in {"1", "true", "yes"}:
        return []

    tidal_range_unit_m = float(os.environ.get("TIDE_COEFFICIENT_RANGE_UNIT_M", "6.10"))
    min_samples = int(os.environ.get("DERIVED_TIDE_MIN_HEIGHT_SAMPLES", "6"))
    if tidal_range_unit_m <= 0:
        return []

    direct_dates = {
        (value.spot_id, _target_date(value, target_tz))
        for value in values
        if value.metric == "tide_coefficient" and value.value is not None
    }

    sea_level_groups: dict[tuple[str, str], list[SourceValue]] = defaultdict(list)
    valid_times_by_group: dict[tuple[str, str], set[datetime]] = defaultdict(set)
    for value in values:
        key = (value.spot_id, _target_date(value, target_tz))
        valid_times_by_group[key].add(value.valid_time)
        if value.metric == "sea_level_height" and value.value is not None:
            sea_level_groups[key].append(value)

    derived: list[SourceValue] = []
    for key, sea_level_values in sea_level_groups.items():
        if key in direct_dates or len(sea_level_values) < min_samples:
            continue

        heights = [value.value for value in sea_level_values if value.value is not None]
        if len(heights) < min_samples:
            continue

        min_height = min(heights)
        max_height = max(heights)
        tidal_range = max_height - min_height
        coefficient = derive_tide_coefficient_from_range(max_height, min_height, tidal_range_unit_m)
        if coefficient is None:
            continue

        spot_id, target_date = key
        for valid_time in sorted(valid_times_by_group[key]):
            derived.append(
                SourceValue(
                    spot_id=spot_id,
                    source_code=DERIVED_TIDE_SOURCE_CODE,
                    valid_time=valid_time,
                    metric="tide_coefficient",
                    value=coefficient,
                    unit="coef",
                    fetched_at=run_time,
                    raw_variable="sea_level_height_daily_range",
                    model=f"range_unit_m={tidal_range_unit_m:g}",
                    resolution_minutes=1440,
                    quality_flags={
                        "formula": "(high_water_m - low_water_m) / range_unit_m * 100",
                        "target_date": target_date,
                        "range_unit_m": tidal_range_unit_m,
                        "height_min_m": min_height,
                        "height_max_m": max_height,
                        "height_range_m": tidal_range,
                        "sample_count": len(heights),
                    },
                )
            )

    return derived


def _metric_provenance(metric: str, spec, metric_values: list[SourceValue]) -> dict[str, Any]:
    sources = sorted({item.source_code for item in metric_values})
    values = [item.value for item in metric_values if item.value is not None]
    mode = os.environ.get("APP_PROVENANCE_MODE", "compact").lower()

    provenance: dict[str, Any] = {
        "metric": metric,
        "unit": spec.unit,
        "method": "circular_mean" if spec.reducer == "circular" else "mean",
        "source_count": len(metric_values),
        "sources": sources,
    }

    if values:
        provenance["min"] = min(values)
        provenance["max"] = max(values)

    if mode == "full":
        provenance["values"] = [_source_trace(item) for item in metric_values]

    return provenance


def _tide_coefficient_reference_range_m() -> float:
    raw_value = os.environ.get("TIDE_APPROX_COEFFICIENT_RANGE_100_M")
    if raw_value is None:
        return TIDE_APPROX_COEFFICIENT_RANGE_100_M

    try:
        value = float(raw_value)
    except ValueError:
        return TIDE_APPROX_COEFFICIENT_RANGE_100_M

    if value <= 0:
        return TIDE_APPROX_COEFFICIENT_RANGE_100_M
    return value


def _approximate_tide_coefficient(tidal_range_m: float) -> float:
    reference_range_m = _tide_coefficient_reference_range_m()
    coefficient = round((tidal_range_m / reference_range_m) * 100.0)
    return float(max(20, min(120, coefficient)))


def _tide_event_type(previous_height: float, current_height: float, next_height: float) -> str | None:
    if current_height >= previous_height and current_height > next_height:
        return "high"
    if current_height <= previous_height and current_height < next_height:
        return "low"
    return None


def _add_tide_derived_fields(rows: list[dict[str, Any]]) -> None:
    rows_by_spot: dict[str, list[dict[str, Any]]] = defaultdict(list)
    rows_by_spot_date: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        if row.get("sea_level_height_m") is None:
            continue
        rows_by_spot[str(row["spot_id"])].append(row)
        rows_by_spot_date[(str(row["spot_id"]), str(row["target_date"]))].append(row)

    for spot_date_rows in rows_by_spot_date.values():
        heights = [float(row["sea_level_height_m"]) for row in spot_date_rows]
        if len(heights) < 2:
            continue

        min_height = min(heights)
        max_height = max(heights)
        tidal_range = max_height - min_height
        coefficient_approx = _approximate_tide_coefficient(tidal_range)
        provenance = {
            "metric": "tide_coefficient_approx",
            "unit": "approx_coef",
            "method": "daily_tidal_range_scaled",
            "source_column": "sea_level_height_m",
            "reference_range_100_m": _tide_coefficient_reference_range_m(),
            "note": "Approximate DATA2LAMER index, not an official SHOM tide coefficient.",
        }

        for row in spot_date_rows:
            row["tide_min_height_m"] = min_height
            row["tide_max_height_m"] = max_height
            row["tide_range_m"] = tidal_range
            row["tide_coefficient_approx"] = coefficient_approx
            row.setdefault("provenance", {})["tide_coefficient_approx"] = provenance

    for spot_rows in rows_by_spot.values():
        spot_rows.sort(key=lambda item: item["valid_time"])
        events: list[dict[str, Any]] = []

        for index in range(1, len(spot_rows) - 1):
            previous_height = float(spot_rows[index - 1]["sea_level_height_m"])
            current_height = float(spot_rows[index]["sea_level_height_m"])
            next_height = float(spot_rows[index + 1]["sea_level_height_m"])
            event_type = _tide_event_type(previous_height, current_height, next_height)
            if event_type is None:
                continue
            events.append(
                {
                    "type": event_type,
                    "time": spot_rows[index]["valid_time"],
                    "height_m": current_height,
                }
            )

        for index, row in enumerate(spot_rows):
            if index + 1 < len(spot_rows):
                current_height = float(row["sea_level_height_m"])
                next_height = float(spot_rows[index + 1]["sea_level_height_m"])
                if next_height > current_height:
                    row["tide_phase"] = "rising"
                elif next_height < current_height:
                    row["tide_phase"] = "falling"
                else:
                    row["tide_phase"] = "slack"

            next_events = [event for event in events if event["time"] >= row["valid_time"]]
            if next_events:
                next_event = next_events[0]
                row["next_tide_event_type"] = next_event["type"]
                row["next_tide_event_time"] = next_event["time"]
                row["next_tide_event_height_m"] = next_event["height_m"]


def consolidate_source_values(values: list[SourceValue], run_time: datetime) -> list[dict[str, Any]]:
    by_spot_time_metric: dict[tuple[str, datetime, str], list[SourceValue]] = defaultdict(list)
    target_tz = ZoneInfo(os.environ.get("FORECAST_TARGET_TIMEZONE", "Europe/Paris"))
    values = [*values, *_derive_tide_coefficients(values, run_time, target_tz)]

    for value in values:
        if value.metric not in METRICS or value.value is None:
            continue
        by_spot_time_metric[(value.spot_id, value.valid_time, value.metric)].append(value)

    row_map: dict[tuple[str, datetime], dict[str, Any]] = {}
    provenance_map: dict[tuple[str, datetime], dict[str, Any]] = defaultdict(dict)
    source_sets: dict[tuple[str, datetime], set[str]] = defaultdict(set)
    for (spot_id, valid_time, metric), metric_values in by_spot_time_metric.items():
        spec = METRICS[metric]
        numbers = [item.value for item in metric_values if item.value is not None]
        if spec.reducer == "circular":
            consolidated_value = _circular_mean_deg(numbers)
        else:
            consolidated_value = _mean(numbers)

        if consolidated_value is None:
            continue

        row_key = (spot_id, valid_time)
        row = row_map.setdefault(
            row_key,
            {
                "spot_id": spot_id,
                "valid_time": valid_time.isoformat(),
                "target_date": valid_time.astimezone(target_tz).date().isoformat(),
                "forecast_run_at": run_time.isoformat(),
                "forecast_horizon_hours": horizon_hours(valid_time, run_time),
            },
        )
        row[spec.column] = consolidated_value

        sources = sorted({item.source_code for item in metric_values})
        source_sets[row_key].update(sources)
        provenance_map[row_key][spec.column] = _metric_provenance(metric, spec, metric_values)

    rows: list[dict[str, Any]] = []
    for row_key, row in row_map.items():
        sources = sorted(source_sets[row_key])
        row["source_count"] = len(sources)
        row["sources"] = sources
        row["provenance"] = provenance_map[row_key]
        rows.append(row)

    _add_tide_derived_fields(rows)
    rows.sort(key=lambda item: (item["spot_id"], item["valid_time"]))
    return rows
