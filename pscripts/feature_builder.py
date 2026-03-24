from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


KEY_COLS = ["spot_id", "date"]

META_COLS = [
    "spot_name",
    "latitude_min",
    "latitude_max",
    "longitude_min",
    "longitude_max",
    "lat_center",
    "lon_center",
    "grid_lat",
    "grid_lon",
    "year",
    "month",
    "dayofyear",
]

STATIC_FEATURES_PATH = os.environ.get("STATIC_FEATURES_PATH", "").strip()


def normalize_date_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce", utc=True).dt.floor("D").dt.tz_localize(None)
    return out


def deduplicate_on_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_date_column(df)
    if out.duplicated(subset=KEY_COLS).any():
        out = out.sort_values(KEY_COLS).drop_duplicates(subset=KEY_COLS, keep="first").copy()
    return out


def merge_prefer_left_meta(left: pd.DataFrame, right: pd.DataFrame, right_name: str) -> pd.DataFrame:
    left = deduplicate_on_keys(left)
    right = deduplicate_on_keys(right)

    overlapping_meta = [c for c in META_COLS if c in left.columns and c in right.columns]
    right_non_meta = [c for c in right.columns if c not in KEY_COLS + overlapping_meta]

    right_trim = right[KEY_COLS + overlapping_meta + right_non_meta].copy()
    rename_map = {c: f"{c}__{right_name}" for c in overlapping_meta}
    right_trim = right_trim.rename(columns=rename_map)

    merged = left.merge(right_trim, on=KEY_COLS, how="left")

    for col in overlapping_meta:
        alt = f"{col}__{right_name}"
        if alt not in merged.columns:
            continue

        if col not in merged.columns:
            merged = merged.rename(columns={alt: col})
        else:
            merged[col] = merged[col].where(merged[col].notna(), merged[alt])
            merged = merged.drop(columns=[alt])

    return merged


def merge_static_by_spot(df: pd.DataFrame) -> pd.DataFrame:
    if not STATIC_FEATURES_PATH:
        return df

    static_path = Path(STATIC_FEATURES_PATH)
    if not static_path.exists():
        print(f"[WARN] STATIC_FEATURES_PATH introuvable: {static_path}")
        return df

    static_df = pd.read_parquet(static_path)
    if "spot_id" not in static_df.columns:
        print("[WARN] spot_id absent du parquet de features statiques.")
        return df

    static_df = static_df.drop_duplicates(subset=["spot_id"]).copy()

    overlapping_non_key = [c for c in static_df.columns if c in df.columns and c != "spot_id"]
    rename_map = {c: f"{c}__static" for c in overlapping_non_key}
    static_df = static_df.rename(columns=rename_map)

    merged = df.merge(static_df, on="spot_id", how="left")

    for original_col in overlapping_non_key:
        static_col = f"{original_col}__static"
        if static_col not in merged.columns:
            continue

        if original_col not in merged.columns:
            merged = merged.rename(columns={static_col: original_col})
        else:
            merged[original_col] = merged[original_col].where(
                merged[original_col].notna(),
                merged[static_col],
            )
            merged = merged.drop(columns=[static_col])

    return merged


def add_training_schema_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # mapping zone -> vocabulaire entraînement
    rename_map = {
        "lat_center": "spot_lat",
        "lon_center": "spot_lon",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})

    # grid_lat/grid_lon fallback
    if "grid_lat" not in out.columns and "spot_lat" in out.columns:
        out["grid_lat"] = out["spot_lat"]
    if "grid_lon" not in out.columns and "spot_lon" in out.columns:
        out["grid_lon"] = out["spot_lon"]

    # cluster / coast_orientation_deg : placeholders si absents
    if "cluster" not in out.columns:
        out["cluster"] = "unknown"

    if "coast_orientation_deg" not in out.columns:
        out["coast_orientation_deg"] = 0.0

    # colonnes temps si absentes
    if "date" in out.columns:
        dt = pd.to_datetime(out["date"], errors="coerce")
        if "year" not in out.columns:
            out["year"] = dt.dt.year
        if "month" not in out.columns:
            out["month"] = dt.dt.month
        if "dayofyear" not in out.columns:
            out["dayofyear"] = dt.dt.dayofyear

    # alias de variables runtime -> noms entraînement
    alias_candidates = {
        "sst": ["sst", "thetao"],
        "salinity": ["salinity", "so"],
        "current_u": ["current_u", "uo"],
        "current_v": ["current_v", "vo"],
        "wave_height": ["wave_height", "VHM0"],
        "wave_period": ["wave_period", "VTM10"],
        "wave_direction": ["wave_direction", "VMDR"],
        "chl_model": ["chl_model", "chl"],
        "phyc": ["phyc"],
        "wind_speed": ["wind_speed"],
        "wind_direction": ["wind_direction"],
        "wind_gusts": ["wind_gusts"],
        "rain_24h": ["rain_24h"],
        "rain_48h": ["rain_48h"],
        "rain_72h": ["rain_72h"],
    }

    for target_col, candidates in alias_candidates.items():
        if target_col in out.columns:
            continue
        for candidate in candidates:
            if candidate in out.columns:
                out[target_col] = out[candidate]
                break

    # variables dérivées
    if "current_speed" not in out.columns and {"current_u", "current_v"}.issubset(out.columns):
        out["current_speed"] = (out["current_u"] ** 2 + out["current_v"] ** 2) ** 0.5

    if "wave_energy" not in out.columns and {"wave_height", "wave_period"}.issubset(out.columns):
        out["wave_energy"] = (out["wave_height"] ** 2) * out["wave_period"]

    if "wave_relative_to_coast" not in out.columns and {"wave_direction", "coast_orientation_deg"}.issubset(out.columns):
        raw = (out["wave_direction"] - out["coast_orientation_deg"]).abs()
        out["wave_relative_to_coast"] = raw.where(raw <= 180, 360 - raw)

    return out


def add_temporal_features_runtime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values(["spot_id", "date"]).reset_index(drop=True)

    lag_features = [
        "wave_height",
        "wave_energy",
        "wave_period",
        "rain_24h",
        "rain_48h",
        "rain_72h",
        "current_speed",
        "chl_model",
        "sst",
    ]

    rolling_features = [
        "wave_height",
        "wave_energy",
        "current_speed",
        "sst",
    ]

    grouped = out.groupby("spot_id", group_keys=False)

    for col in lag_features:
        if col in out.columns:
            out[f"{col}_lag_1"] = grouped[col].shift(1)

    for col in rolling_features:
        if col in out.columns:
            out[f"{col}_roll3_mean"] = grouped[col].transform(
                lambda s: s.shift(1).rolling(3, min_periods=1).mean()
            )

    return out


def build_feature_frame(
    phy_df: pd.DataFrame,
    wav_df: pd.DataFrame,
    bgc_df: pd.DataFrame,
    meteo_df: pd.DataFrame,
) -> pd.DataFrame:
    # socle = météo, pour éviter l'explosion de lignes liée aux outer joins
    df = deduplicate_on_keys(meteo_df)

    df = merge_prefer_left_meta(df, phy_df, "phy")
    df = merge_prefer_left_meta(df, wav_df, "wav")
    df = merge_prefer_left_meta(df, bgc_df, "bgc")
    df = merge_static_by_spot(df)

    df = add_training_schema_columns(df)
    df = add_temporal_features_runtime(df)

    df = df.sort_values(["spot_id", "date"]).reset_index(drop=True)
    return df