from __future__ import annotations

import pandas as pd

from config.settings import (
    FINAL_DATASET_FILE,
    ML_DIR,
    ML_READY_FILE,
    ML_READY_SAMPLE_FILE,
    ML_READY_SUMMARY_FILE,
    ML_READY_SPOT_COUNTS_FILE,
)

from src.utils.io_utils import (
    ensure_dir,
    assert_file_exists,
    read_parquet,
    save_parquet_bundle,
    write_csv,
)
from src.utils.logging_utils import (
    log_kv,
    log_df_head,
    log_file_written,
    log_dataset_overview,
)
from src.utils.summary_utils import (
    build_column_summary,
    build_spot_date_coverage,
)


BASE_COLS = [
    "date",
    "spot_id",
    "spot_name",
    "cluster",
    "spot_lat",
    "spot_lon",
    "grid_lat",
    "grid_lon",
    "coast_orientation_deg",
    "year",
    "month",
    "dayofyear",
    "zsd",
]

REQUIRED_COLS = [
    "sst",
    "salinity",
    "current_u",
    "current_v",
    "current_speed",
    "wave_height",
    "wave_period",
    "wave_direction",
    "wave_energy",
    "wave_relative_to_coast",
    "chl_model",
    "phyc",
    "wind_speed",
    "wind_direction",
    "wind_gusts",
    "rain_24h",
    "rain_48h",
    "rain_72h",
    "wind_relative_to_coast",
]

OPTIONAL_COLS = [
    "bathy_point",
    "bathy_mean_150m",
    "bathy_mean_300m",
    "bathy_std_150m",
    "bathy_std_300m",
    "bathy_min_150m",
    "bathy_min_300m",
    "bathy_max_150m",
    "bathy_max_300m",
    "slope_mean_150m",
    "slope_mean_300m",
    "slope_std_150m",
    "slope_std_300m",
    "slope_max_150m",
    "slope_max_300m",
    "dist_coast_m",
    "static_data_ok",
]


def validate_input_columns(df: pd.DataFrame) -> None:
    missing_base = [c for c in BASE_COLS if c not in df.columns]
    if missing_base:
        raise KeyError(f"Colonnes de base manquantes: {missing_base}")

    missing_required = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_required:
        raise KeyError(f"Colonnes required manquantes: {missing_required}")


def build_ml_dataset(df: pd.DataFrame) -> pd.DataFrame:
    validate_input_columns(df)

    selected_cols = BASE_COLS + REQUIRED_COLS + [c for c in OPTIONAL_COLS if c in df.columns]
    selected_cols = list(dict.fromkeys(selected_cols))

    out = df[selected_cols].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.floor("D")

    required_non_null = ["date", "zsd"] + REQUIRED_COLS
    out = out.dropna(subset=required_non_null).copy()

    out = out.sort_values(["spot_id", "date"]).reset_index(drop=True)
    return out


def main() -> None:
    ensure_dir(ML_DIR)
    assert_file_exists(FINAL_DATASET_FILE, label="INPUT_FILE")

    log_kv("INPUT_FILE", FINAL_DATASET_FILE)

    df = read_parquet(FINAL_DATASET_FILE, label="INPUT_FILE")

    print(f"Shape brut: {df.shape}")
    print(f"Nb spots brut: {df['spot_id'].nunique()}")
    dt = pd.to_datetime(df["date"], errors="coerce")
    print(f"Date min/max brut: {dt.min()} -> {dt.max()}")

    ml_df = build_ml_dataset(df)

    kept = len(ml_df)
    total = len(df)
    pct = round((kept / total) * 100, 1) if total else 0.0
    print(f"Lignes gardées après filtre ML: {kept}/{total} ({pct}%)")

    summary_df = build_column_summary(ml_df)
    spot_counts_df = build_spot_date_coverage(ml_df, spot_col="spot_id", date_col="date")

    written = save_parquet_bundle(
        df=ml_df,
        parquet_path=ML_READY_FILE,
        sample_path=ML_READY_SAMPLE_FILE,
        summary_df=summary_df,
        summary_path=ML_READY_SUMMARY_FILE,
        index=False,
    )

    write_csv(spot_counts_df, ML_READY_SPOT_COUNTS_FILE, index=False)

    print()
    for path in written.values():
        log_file_written(path)
    log_file_written(ML_READY_SPOT_COUNTS_FILE)

    print()
    log_df_head(ml_df, n=5)

    print()
    print(f"Shape final: {ml_df.shape}")
    print(f"Nb spots final: {ml_df['spot_id'].nunique()}")
    dt_final = pd.to_datetime(ml_df["date"], errors="coerce")
    print(f"Date min/max final: {dt_final.min()} -> {dt_final.max()}")

    print("\nJours par spot (10 plus faibles):")
    print(spot_counts_df.head(10))

    print("\nNaN % top 15:")
    print(summary_df.head(15))


if __name__ == "__main__":
    main()