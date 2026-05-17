from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricSpec:
    metric: str
    column: str
    unit: str
    reducer: str = "mean"


METRICS: dict[str, MetricSpec] = {
    "wind_speed": MetricSpec("wind_speed", "wind_speed_ms", "m/s"),
    "wind_gusts": MetricSpec("wind_gusts", "wind_gusts_ms", "m/s"),
    "wind_direction": MetricSpec("wind_direction", "wind_direction_deg", "deg", "circular"),
    "air_temperature": MetricSpec("air_temperature", "air_temperature_c", "degC"),
    "relative_humidity": MetricSpec("relative_humidity", "relative_humidity_pct", "%"),
    "dew_point": MetricSpec("dew_point", "dew_point_c", "degC"),
    "pressure_msl": MetricSpec("pressure_msl", "pressure_msl_hpa", "hPa"),
    "surface_pressure": MetricSpec("surface_pressure", "surface_pressure_hpa", "hPa"),
    "cloud_cover": MetricSpec("cloud_cover", "cloud_cover_pct", "%"),
    "cloud_cover_low": MetricSpec("cloud_cover_low", "cloud_cover_low_pct", "%"),
    "cloud_cover_mid": MetricSpec("cloud_cover_mid", "cloud_cover_mid_pct", "%"),
    "cloud_cover_high": MetricSpec("cloud_cover_high", "cloud_cover_high_pct", "%"),
    "precipitation": MetricSpec("precipitation", "precipitation_mm", "mm"),
    "weather_visibility": MetricSpec("weather_visibility", "weather_visibility_m", "m"),
    "wave_height": MetricSpec("wave_height", "wave_height_m", "m"),
    "wave_period": MetricSpec("wave_period", "wave_period_s", "s"),
    "wave_direction": MetricSpec("wave_direction", "wave_direction_deg", "deg", "circular"),
    "wind_wave_height": MetricSpec("wind_wave_height", "wind_wave_height_m", "m"),
    "wind_wave_period": MetricSpec("wind_wave_period", "wind_wave_period_s", "s"),
    "wind_wave_direction": MetricSpec("wind_wave_direction", "wind_wave_direction_deg", "deg", "circular"),
    "swell_wave_height": MetricSpec("swell_wave_height", "swell_wave_height_m", "m"),
    "swell_wave_period": MetricSpec("swell_wave_period", "swell_wave_period_s", "s"),
    "swell_wave_direction": MetricSpec("swell_wave_direction", "swell_wave_direction_deg", "deg", "circular"),
    "secondary_swell_wave_height": MetricSpec(
        "secondary_swell_wave_height", "secondary_swell_wave_height_m", "m"
    ),
    "secondary_swell_wave_period": MetricSpec(
        "secondary_swell_wave_period", "secondary_swell_wave_period_s", "s"
    ),
    "secondary_swell_wave_direction": MetricSpec(
        "secondary_swell_wave_direction",
        "secondary_swell_wave_direction_deg",
        "deg",
        "circular",
    ),
    "water_temperature": MetricSpec("water_temperature", "water_temperature_c", "degC"),
    "sea_level_height": MetricSpec("sea_level_height", "sea_level_height_m", "m"),
    "tide_coefficient": MetricSpec("tide_coefficient", "tide_coefficient", "coef"),
    "current_speed": MetricSpec("current_speed", "current_speed_ms", "m/s"),
    "current_direction": MetricSpec("current_direction", "current_direction_deg", "deg", "circular"),
    "salinity": MetricSpec("salinity", "salinity_psu", "psu"),
    "chlorophyll": MetricSpec("chlorophyll", "chlorophyll_mg_m3", "mg/m3"),
    "light_attenuation": MetricSpec("light_attenuation", "light_attenuation_m1", "1/m"),
}


DIRECTION_METRICS = {
    metric for metric, spec in METRICS.items() if spec.reducer == "circular"
}
