from __future__ import annotations

import math
import os
import time
from pathlib import Path

import pandas as pd
import requests

from config.settings import CONFIG_DATA_DIR, FEATURES_DIR, SPOTS_SOURCE_FILE
from src.utils.io_utils import ensure_dir, read_csv, save_parquet_bundle
from src.utils.logging_utils import log_df_head, log_file_written, log_kv
from src.utils.summary_utils import build_column_summary

SPOTS_FILE = Path(os.environ.get("SPOTS_FILE_OVERRIDE", str(SPOTS_SOURCE_FILE)))

START_DATE = "2024-01-01"
END_DATE = "2026-03-16"
TIMEZONE = "Europe/Paris"
HOURLY_VARS = ["wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "precipitation"]
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
REQUEST_TIMEOUT = 60
SLEEP_BETWEEN_CALLS_SEC = 0.2

OUT_FILE = FEATURES_DIR / "features_meteo_daily_by_spot_2024_2026.parquet"
OUT_SAMPLE_FILE = FEATURES_DIR / "features_meteo_daily_by_spot_sample.parquet"
OUT_SUMMARY_FILE = FEATURES_DIR / "features_meteo_daily_by_spot_summary.csv"
REQUIRED_SPOT_COLS = ["spot_id", "name", "lat", "lon"]
OPTIONAL_SPOT_COLS = ["coast_orientation_deg", "cluster"]


def load_spots(spots_file: Path) -> pd.DataFrame:
    spots = read_csv(spots_file, label="SPOTS_FILE")
    missing = [c for c in REQUIRED_SPOT_COLS if c not in spots.columns]
    if missing: raise ValueError(f"Colonnes manquantes dans {spots_file}: {missing}")
    return spots.copy()


def fetch_hourly_history(lat: float, lon: float) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": TIMEZONE,
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
    }
    response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if "hourly" not in payload or "time" not in payload["hourly"]:
        raise ValueError("Réponse Open-Meteo invalide ou vide.")
    df = pd.DataFrame(payload["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    return df


def circular_mean_deg(series: pd.Series) -> float:
    s = pd.to_numeric(series.dropna(), errors="coerce").dropna().astype(float)
    if s.empty: return float("nan")
    rad = s * (math.pi / 180.0)
    angle = math.degrees(math.atan2(rad.map(math.sin).mean(), rad.map(math.cos).mean()))
    return (angle + 360.0) % 360.0


def aggregate_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    df = hourly_df.copy(); df["date"] = df["time"].dt.floor("D")
    agg = df.groupby("date", as_index=False).agg(
        wind_speed=("wind_speed_10m", "mean"),
        wind_gusts=("wind_gusts_10m", "max"),
        precipitation=("precipitation", "sum"),
    )
    wind_dir = df.groupby("date")["wind_direction_10m"].apply(circular_mean_deg).reset_index(name="wind_direction")
    agg = agg.merge(wind_dir, on="date", how="left").sort_values("date").reset_index(drop=True)
    agg["rain_24h"] = agg["precipitation"]
    agg["rain_48h"] = agg["precipitation"].rolling(window=2, min_periods=1).sum()
    agg["rain_72h"] = agg["precipitation"].rolling(window=3, min_periods=1).sum()
    agg["year"] = agg["date"].dt.year; agg["month"] = agg["date"].dt.month; agg["dayofyear"] = agg["date"].dt.dayofyear
    return agg


def add_spot_metadata(df: pd.DataFrame, spot: pd.Series) -> pd.DataFrame:
    df = df.copy()
    df["spot_id"] = spot["spot_id"]; df["spot_name"] = spot["name"]
    df["spot_lat"] = float(spot["lat"]); df["spot_lon"] = float(spot["lon"])
    if "coast_orientation_deg" in spot.index:
        df["coast_orientation_deg"] = spot["coast_orientation_deg"]
        if "wind_direction" in df.columns:
            diff = (df["wind_direction"] - df["coast_orientation_deg"] + 180) % 360 - 180
            df["wind_relative_to_coast"] = diff.abs()
    if "cluster" in spot.index:
        df["cluster"] = spot["cluster"]
    return df


def build_spot_dataframe(spots: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    total = len(spots)
    for i, (_, spot) in enumerate(spots.iterrows(), start=1):
        print(f"[METEO] [{i}/{total}] {spot['spot_id']} ({spot['name']})", flush=True)
        try:
            hourly = fetch_hourly_history(float(spot["lat"]), float(spot["lon"]))
            daily = add_spot_metadata(aggregate_daily(hourly), spot)
            frames.append(daily)
            print(f"[METEO] [{i}/{total}] OK | {daily['date'].min()} -> {daily['date'].max()} | rows={len(daily)}", flush=True)
        except Exception as exc:
            print(f"[METEO] [{i}/{total}] ERREUR: {exc}", flush=True)
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    if not frames: raise ValueError("Aucune donnée météo spot extraite")
    return pd.concat(frames, ignore_index=True).sort_values(["spot_id", "date"]).reset_index(drop=True)


def main() -> None:
    ensure_dir(FEATURES_DIR); ensure_dir(CONFIG_DATA_DIR)
    log_kv("SPOTS_FILE", SPOTS_FILE); log_kv("START_DATE", START_DATE); log_kv("END_DATE", END_DATE); log_kv("TIMEZONE", TIMEZONE)
    spots = load_spots(SPOTS_FILE)
    print("Spots:", len(spots))
    df = build_spot_dataframe(spots)
    summary_df = build_column_summary(df)
    written = save_parquet_bundle(df=df, parquet_path=OUT_FILE, sample_path=OUT_SAMPLE_FILE, summary_df=summary_df, summary_path=OUT_SUMMARY_FILE, index=False)
    print()
    for path in written.values(): log_file_written(path)
    print(); log_df_head(df, n=5)
    print("Shape:", df.shape); print("Nb spots:", df["spot_id"].nunique()); print("Date min/max:", df["date"].min(), df["date"].max())


if __name__ == "__main__":
    main()
