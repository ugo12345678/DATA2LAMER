from __future__ import annotations

import pandas as pd

from config.settings import (
    FINAL_DIR,
    FINAL_DATASET_FILE,
    TEMPORAL_DATASET_FILE,
    TEMPORAL_DATASET_SAMPLE_FILE,
    TEMPORAL_DATASET_SUMMARY_FILE,
)
from src.utils.io_utils import (
    ensure_dir,
    assert_file_exists,
    read_parquet,
    save_parquet_bundle,
)
from src.utils.logging_utils import log_df_head, log_file_written, log_kv
from src.utils.summary_utils import build_column_summary

INPUT_FILE = FINAL_DATASET_FILE
OUT_FILE = TEMPORAL_DATASET_FILE
OUT_SAMPLE_FILE = TEMPORAL_DATASET_SAMPLE_FILE
OUT_SUMMARY_FILE = TEMPORAL_DATASET_SUMMARY_FILE

KEY_COLS = ["spot_id", "date"]

LAG_FEATURES = [
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

ROLLING_FEATURES = [
    "wave_height",
    "wave_energy",
    "current_speed",
    "sst",
]

TARGET_LAG_FEATURES = [
    # "zsd",
]

LAGS = [1]
ROLLING_WINDOWS = [3]


def add_lag_features(df: pd.DataFrame, cols: list[str], lags: list[int]) -> pd.DataFrame:
    df = df.copy()
    grouped = df.groupby("spot_id", group_keys=False)

    for col in cols:
        if col not in df.columns:
            print(f"[WARN] lag ignoré, colonne absente: {col}")
            continue
        for lag in lags:
            df[f"{col}_lag_{lag}"] = grouped[col].shift(lag)

    return df


def add_rolling_features(df: pd.DataFrame, cols: list[str], windows: list[int]) -> pd.DataFrame:
    df = df.copy()

    for col in cols:
        if col not in df.columns:
            print(f"[WARN] rolling ignoré, colonne absente: {col}")
            continue

        for window in windows:
            df[f"{col}_roll{window}_mean"] = (
                df.groupby("spot_id")[col]
                .transform(lambda s: s.shift(1).rolling(window, min_periods=1).mean())
            )

    return df


def main() -> None:
    ensure_dir(FINAL_DIR)
    assert_file_exists(INPUT_FILE, label="INPUT_FILE")

    log_kv("INPUT_FILE", INPUT_FILE)

    df = read_parquet(INPUT_FILE, label="INPUT_FILE")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(KEY_COLS).reset_index(drop=True)

    print("Shape brut:", df.shape)
    print("Nb spots brut:", df["spot_id"].nunique())
    print("Date min/max brut:", df["date"].min(), "->", df["date"].max())

    df = add_lag_features(df, cols=LAG_FEATURES, lags=LAGS)
    df = add_rolling_features(df, cols=ROLLING_FEATURES, windows=ROLLING_WINDOWS)
    df = add_lag_features(df, cols=TARGET_LAG_FEATURES, lags=LAGS)

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
    print("Shape final:", df.shape)
    print("Nb spots final:", df["spot_id"].nunique())
    print("Date min/max final:", df["date"].min(), "->", df["date"].max())


if __name__ == "__main__":
    main()