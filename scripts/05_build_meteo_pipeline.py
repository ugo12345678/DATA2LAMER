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

REQUEST_TIMEOUT = 120
SLEEP_BETWEEN_BATCHES_SEC = 2.0

MAX_RETRIES = 6
BACKOFF_BASE_SEC = 3.0
BACKOFF_MAX_SEC = 60.0

BATCH_SIZE = 15

OUT_FILE = FEATURES_DIR / "features_meteo_daily_by_spot_2024_2026.parquet"
OUT_SAMPLE_FILE = FEATURES_DIR / "features_meteo_daily_by_spot_sample.parquet"
OUT_SUMMARY_FILE = FEATURES_DIR / "features_meteo_daily_by_spot_summary.csv"

REQUIRED_SPOT_COLS = ["spot_id", "name", "lat", "lon"]
OPTIONAL_SPOT_COLS = ["coast_orientation_deg", "cluster"]

SESSION = requests.Session()


def load_spots(spots_file: Path) -> pd.DataFrame:
    spots = read_csv(spots_file, label="SPOTS_FILE")
    missing = [c for c in REQUIRED_SPOT_COLS if c not in spots.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans {spots_file}: {missing}")
    return spots.copy()


def chunk_dataframe(df: pd.DataFrame, chunk_size: int):
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size].copy()


def build_request_params_for_batch(spots_batch: pd.DataFrame) -> dict:
    latitudes = ",".join(spots_batch["lat"].astype(float).map(str).tolist())
    longitudes = ",".join(spots_batch["lon"].astype(float).map(str).tolist())

    return {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(HOURLY_VARS),
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
                wait_s = min(BACKOFF_MAX_SEC, BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
                print(
                    f"[METEO] 429 Too Many Requests -> retry {attempt}/{MAX_RETRIES} dans {wait_s:.1f}s",
                    flush=True,
                )
                time.sleep(wait_s)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt == MAX_RETRIES:
                break

            wait_s = min(BACKOFF_MAX_SEC, BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
            print(
                f"[METEO] Erreur réseau -> retry {attempt}/{MAX_RETRIES} dans {wait_s:.1f}s | {exc}",
                flush=True,
            )
            time.sleep(wait_s)

    raise RuntimeError(f"Échec Open-Meteo après {MAX_RETRIES} tentatives") from last_exc


def normalize_batch_payload(payload: dict | list, expected_count: int) -> list[dict]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = [payload]
    else:
        raise ValueError("Format de réponse Open-Meteo inattendu.")

    if len(items) != expected_count:
        raise ValueError(
            f"Nombre de réponses Open-Meteo inattendu: reçu={len(items)} attendu={expected_count}"
        )

    return items


def hourly_payload_to_dataframe(payload_item: dict) -> pd.DataFrame:
    if "hourly" not in payload_item or "time" not in payload_item["hourly"]:
        raise ValueError("Réponse Open-Meteo invalide ou vide.")

    df = pd.DataFrame(payload_item["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    return df


def fetch_hourly_history_batch(spots_batch: pd.DataFrame) -> list[pd.DataFrame]:
    params = build_request_params_for_batch(spots_batch)
    payload = get_with_retry(params)
    items = normalize_batch_payload(payload, expected_count=len(spots_batch))
    return [hourly_payload_to_dataframe(item) for item in items]


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


def add_spot_metadata(df: pd.DataFrame, spot: pd.Series) -> pd.DataFrame:
    df = df.copy()
    df["spot_id"] = spot["spot_id"]
    df["spot_name"] = spot["name"]
    df["spot_lat"] = float(spot["lat"])
    df["spot_lon"] = float(spot["lon"])

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
    processed = 0
    batch_total = math.ceil(total / BATCH_SIZE)

    for batch_idx, spots_batch in enumerate(chunk_dataframe(spots, BATCH_SIZE), start=1):
        batch_start = processed + 1
        batch_end = processed + len(spots_batch)

        print(
            f"[METEO] [BATCH {batch_idx}/{batch_total}] spots {batch_start}-{batch_end}/{total}",
            flush=True,
        )

        try:
            hourly_dfs = fetch_hourly_history_batch(spots_batch)

            for (_, spot), hourly in zip(spots_batch.iterrows(), hourly_dfs):
                i = processed + 1
                print(f"[METEO] [{i}/{total}] {spot['spot_id']} ({spot['name']})", flush=True)

                try:
                    daily = add_spot_metadata(aggregate_daily(hourly), spot)
                    frames.append(daily)

                    print(
                        f"[METEO] [{i}/{total}] OK | {daily['date'].min()} -> {daily['date'].max()} | rows={len(daily)}",
                        flush=True,
                    )
                except Exception as exc:
                    print(f"[METEO] [{i}/{total}] ERREUR POST-TRAITEMENT: {exc}", flush=True)

                processed += 1

        except Exception as exc:
            print(
                f"[METEO] [BATCH {batch_idx}/{batch_total}] ERREUR BATCH: {exc}",
                flush=True,
            )

            # fallback spot par spot si le batch complet échoue
            for _, spot in spots_batch.iterrows():
                i = processed + 1
                print(f"[METEO] [{i}/{total}] {spot['spot_id']} ({spot['name']}) [fallback]", flush=True)

                try:
                    params = {
                        "latitude": float(spot["lat"]),
                        "longitude": float(spot["lon"]),
                        "start_date": START_DATE,
                        "end_date": END_DATE,
                        "hourly": ",".join(HOURLY_VARS),
                        "timezone": TIMEZONE,
                        "wind_speed_unit": "ms",
                        "precipitation_unit": "mm",
                    }

                    payload = get_with_retry(params)
                    hourly = hourly_payload_to_dataframe(payload)
                    daily = add_spot_metadata(aggregate_daily(hourly), spot)
                    frames.append(daily)

                    print(
                        f"[METEO] [{i}/{total}] OK fallback | {daily['date'].min()} -> {daily['date'].max()} | rows={len(daily)}",
                        flush=True,
                    )
                except Exception as spot_exc:
                    print(f"[METEO] [{i}/{total}] ERREUR fallback: {spot_exc}", flush=True)

                processed += 1

        time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

    if not frames:
        raise ValueError("Aucune donnée météo spot extraite")

    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["spot_id", "date"])
        .reset_index(drop=True)
    )


def main() -> None:
    ensure_dir(FEATURES_DIR)
    ensure_dir(CONFIG_DATA_DIR)

    log_kv("SPOTS_FILE", SPOTS_FILE)
    log_kv("START_DATE", START_DATE)
    log_kv("END_DATE", END_DATE)
    log_kv("TIMEZONE", TIMEZONE)
    log_kv("BATCH_SIZE", BATCH_SIZE)

    spots = load_spots(SPOTS_FILE)
    print("Spots:", len(spots))

    df = build_spot_dataframe(spots)
    summary_df = build_column_summary(df)

    written = save_parquet_bundle(
        df=df,
        parquet_path=OUT_FILE,
        sample_path=OUT_SAMPLE_FILE,
        summary_df=summary_df,
        summary_path=OUT_SUMMARY_FILE,
        index=False,
    )

    print()
    for path in written.values():
        log_file_written(path)

    print()
    log_df_head(df, n=5)
    print("Shape:", df.shape)
    print("Nb spots:", df["spot_id"].nunique())
    print("Date min/max:", df["date"].min(), df["date"].max())


if __name__ == "__main__":
    main()