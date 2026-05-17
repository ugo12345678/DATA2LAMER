from __future__ import annotations

import math
from typing import Any


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def normalize_unit_label(unit: str | None) -> str:
    return (unit or "").replace(" ", "").lower()


def convert_velocity_to_ms(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None

    normalized = normalize_unit_label(unit)
    if normalized in {"m/s", "ms", "meter/second", "metre/second"}:
        return value
    if normalized in {"km/h", "kmh", "kilometer/hour", "kilometre/hour"}:
        return value / 3.6
    if normalized in {"kn", "kt", "kts", "knots"}:
        return value * 0.514444
    if normalized in {"mph"}:
        return value * 0.44704
    return value


def normalize_metric_value(metric: str, value: Any, unit: str | None) -> tuple[float | None, str]:
    number = to_float(value)

    if metric in {"wind_speed", "wind_gusts", "current_speed"}:
        return convert_velocity_to_ms(number, unit), "m/s"

    if metric in {
        "wind_direction",
        "wave_direction",
        "wind_wave_direction",
        "swell_wave_direction",
        "secondary_swell_wave_direction",
        "current_direction",
    }:
        if number is None:
            return None, "deg"
        return number % 360.0, "deg"

    if metric in {
        "air_temperature",
        "dew_point",
        "water_temperature",
    }:
        return number, "degC"

    if metric in {
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
        "relative_humidity",
    }:
        return number, "%"

    if metric in {
        "wave_height",
        "wind_wave_height",
        "swell_wave_height",
        "secondary_swell_wave_height",
        "sea_level_height",
        "weather_visibility",
    }:
        return number, "m"

    if metric in {
        "wave_period",
        "wind_wave_period",
        "swell_wave_period",
        "secondary_swell_wave_period",
    }:
        return number, "s"

    if metric == "precipitation":
        return number, "mm"

    if metric in {"pressure_msl", "surface_pressure"}:
        return number, "hPa"

    if metric == "tide_coefficient":
        return number, "coef"

    if metric == "salinity":
        return number, "psu"

    return number, unit or ""
