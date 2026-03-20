from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


KEY_COLS = ["zone_id", "date"]

META_COLS = [
    "zone_name",
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
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.floor("D")
    return out


def deduplicate_on_keys(df: pd.DataFrame) -> pd.DataFrame:
    if df.duplicated(subset=KEY_COLS).any():
        df = df.sort_values(KEY_COLS).drop_duplicates(subset=KEY_COLS, keep="first").copy()
    return df


def merge_prefer_left_meta(left: pd.DataFrame, right: pd.DataFrame, right_name: str) -> pd.DataFrame:
    left = deduplicate_on_keys(normalize_date_column(left))
    right = deduplicate_on_keys(normalize_date_column(right))

    overlapping_meta = [c for c in META_COLS if c in left.columns and c in right.columns]
    right_non_meta = [c for c in right.columns if c not in KEY_COLS + overlapping_meta]

    right_trim = right[KEY_COLS + overlapping_meta + right_non_meta].copy()
    rename_map = {c: f"{c}__{right_name}" for c in overlapping_meta}
    right_trim = right_trim.rename(columns=rename_map)

    merged = left.merge(right_trim, on=KEY_COLS, how="outer")

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


def merge_static_by_zone(df: pd.DataFrame) -> pd.DataFrame:
    if not STATIC_FEATURES_PATH:
        return df

    static_path = Path(STATIC_FEATURES_PATH)
    if not static_path.exists():
        return df

    static_df = pd.read_parquet(static_path)
    if "zone_id" not in static_df.columns:
        return df

    static_df = static_df.drop_duplicates(subset=["zone_id"]).copy()

    overlapping_non_key = [c for c in static_df.columns if c in df.columns and c != "zone_id"]
    rename_map = {c: f"{c}__static" for c in overlapping_non_key}
    static_df = static_df.rename(columns=rename_map)

    merged = df.merge(static_df, on="zone_id", how="left")

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


def build_feature_frame(
    phy_df: pd.DataFrame,
    wav_df: pd.DataFrame,
    bgc_df: pd.DataFrame,
    meteo_df: pd.DataFrame,
) -> pd.DataFrame:
    df = normalize_date_column(meteo_df)
    df = merge_prefer_left_meta(df, phy_df, "phy")
    df = merge_prefer_left_meta(df, wav_df, "wav")
    df = merge_prefer_left_meta(df, bgc_df, "bgc")
    df = merge_static_by_zone(df)

    df = df.sort_values(["zone_id", "date"]).reset_index(drop=True)
    return df