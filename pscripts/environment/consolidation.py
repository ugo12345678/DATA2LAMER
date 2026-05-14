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


def consolidate_source_values(values: list[SourceValue], run_time: datetime) -> list[dict[str, Any]]:
    by_spot_time_metric: dict[tuple[str, datetime, str], list[SourceValue]] = defaultdict(list)

    for value in values:
        if value.metric not in METRICS or value.value is None:
            continue
        by_spot_time_metric[(value.spot_id, value.valid_time, value.metric)].append(value)

    row_map: dict[tuple[str, datetime], dict[str, Any]] = {}
    provenance_map: dict[tuple[str, datetime], dict[str, Any]] = defaultdict(dict)
    source_sets: dict[tuple[str, datetime], set[str]] = defaultdict(set)
    target_tz = ZoneInfo(os.environ.get("FORECAST_TARGET_TIMEZONE", "Europe/Paris"))

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

    rows.sort(key=lambda item: (item["spot_id"], item["valid_time"]))
    return rows
