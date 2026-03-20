from __future__ import annotations

import math
import os
import time

import pandas as pd
import requests


FORECAST_DAYS = int(os.environ.get("FORECAST_DAYS", "7"))
TIMEZONE = os.environ.get("FORECAST_TIMEZONE", "Europe/Paris")
BATCH_SIZE = int(os.environ.get("OPEN_METEO_BATCH_SIZE", "20"))
REQUEST_TIMEOUT = 120
MAX_RETRIES = 5
SLEEP_BETWEEN_BATCHES_SEC = 1.0

HOURLY_VARS = [
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "precipitation",
    "cloud_cover",
    "temperature_2m",
]

BASE_URL = "https://api.open-meteo.com/v1/forecast"
SESSION = requests.Session()


def chunk_dataframe(df: pd.DataFrame, chunk_size: int):
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size].copy()


def build_request_params_for_batch(zones_batch: pd.DataFrame) -> dict:
    return {
        "latitude": ",".join(zones_batch["lat_center"].astype(float).map(str).tolist()),
        "longitude": ",".join(zones_batch["lon_center"].astype(float).map(str).tolist()),
        "hourly": ",".join(HOURLY_VARS),
        "forecast_days": FORECAST_DAYS,
        "timezone": TIMEZONE,
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
    }


def get_with_retry(params: dict) -> dict | list:
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code == 429:
                time.sleep(min(30, 2 ** attempt))
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt == MAX_RETRIES:
                break
            time.sleep(min(30, 2 ** attempt))

    raise RuntimeError("Échec Open-Meteo forecast") from last_exc


def normalize_batch_payload(payload: dict | list, expected_count: int) -> list[dict]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = [payload]
    else:
        raise ValueError("Format Open-Meteo inattendu")

    if len(items) != expected_count:
        raise ValueError(f"Nombre de réponses inattendu: reçu={len(items)} attendu={expected_count}")

    return items


def hourly_payload_to_dataframe(payload_item: dict) -> pd.DataFrame:
    hourly = payload_item.get("hourly", {})
    if "time" not in hourly:
        raise ValueError("Réponse hourly invalide")

    df = pd.DataFrame(hourly)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


def circular_mean_deg(series: pd.Series) -> float:
    s = pd.to_numeric(series.dropna(), errors="coerce").dropna().astype(float)
    if s.empty:
        return float("nan")

    rad = s * (math.pi / 180.0)
    angle = math.degrees(math.atan2(rad.map(math.sin).mean(), rad.map(math.cos).mean()))
    return (angle + 360.0) % 360.0


def aggregate_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    df = hourly_df.copy()
    df["date"] = df["time"].dt.floor("D")

    agg = df.groupby("date", as_index=False).agg(
        wind_speed=("wind_speed_10m", "mean"),
        wind_gusts=("wind_gusts_10m", "max"),
        precipitation=("precipitation", "sum"),
        cloud_cover=("cloud_cover", "mean"),
        temperature_2m=("temperature_2m", "mean"),
    )

    wind_dir = (
        df.groupby("date")["wind_direction_10m"]
        .apply(circular_mean_deg)
        .reset_index(name="wind_direction")
    )

    agg = agg.merge(wind_dir, on="date", how="left").sort_values("date").reset_index(drop=True)
    agg["rain_24h"] = agg["precipitation"]
    agg["rain_48h"] = agg["precipitation"].rolling(window=2, min_periods=1).sum()
    agg["rain_72h"] = agg["precipitation"].rolling(window=3, min_periods=1).sum()

    agg["year"] = agg["date"].dt.year
    agg["month"] = agg["date"].dt.month
    agg["dayofyear"] = agg["date"].dt.dayofyear
    return agg


def add_zone_metadata(df: pd.DataFrame, zone: pd.Series) -> pd.DataFrame:
    out = df.copy()
    out["zone_id"] = zone["zone_id"]
    out["zone_name"] = zone["zone_name"]
    out["latitude_min"] = float(zone["latitude_min"])
    out["latitude_max"] = float(zone["latitude_max"])
    out["longitude_min"] = float(zone["longitude_min"])
    out["longitude_max"] = float(zone["longitude_max"])
    out["lat_center"] = float(zone["lat_center"])
    out["lon_center"] = float(zone["lon_center"])
    return out


def fetch_meteo_forecast(zones: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for zones_batch in chunk_dataframe(zones, BATCH_SIZE):
        params = build_request_params_for_batch(zones_batch)
        payload = get_with_retry(params)
        items = normalize_batch_payload(payload, expected_count=len(zones_batch))

        for (_, zone), item in zip(zones_batch.iterrows(), items):
            hourly = hourly_payload_to_dataframe(item)
            daily = aggregate_daily(hourly)
            daily = add_zone_metadata(daily, zone)
            frames.append(daily)

        time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

    if not frames:
        raise ValueError("Aucune donnée météo forecast produite")

    return pd.concat(frames, ignore_index=True).sort_values(["zone_id", "date"]).reset_index(drop=True)