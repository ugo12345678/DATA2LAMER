from __future__ import annotations

import pandas as pd

from config.settings import (
    TARGET_FILE,
    PHY_FILE,
    WAV_FILE,
    BGC_FILE,
    METEO_FILE,
    STATIC_FILE,
    FINAL_DIR,
    FINAL_DATASET_FILE,
    FINAL_DATASET_SAMPLE_FILE,
    FINAL_DATASET_SUMMARY_FILE,
)

from src.utils.io_utils import (
    ensure_dir,
    assert_file_exists,
    read_parquet,
    save_parquet_bundle,
)
from src.utils.logging_utils import (
    log_kv,
    log_shape,
    log_df_head,
    log_file_written,
    log_warning,
    log_dataset_overview,
)
from src.utils.summary_utils import build_column_summary


KEY_COLS = ["date", "spot_id"]

META_COLS = [
    "spot_name",
    "spot_lat",
    "spot_lon",
    "grid_lat",
    "grid_lon",
    "coast_orientation_deg",
    "cluster",
    "year",
    "month",
    "dayofyear",
]

BGC_DROP_IF_MISSING_OK = [
    "light_attenuation",
]


def load_input_datasets() -> dict[str, pd.DataFrame]:
    files = {
        "target": TARGET_FILE,
        "phy": PHY_FILE,
        "wav": WAV_FILE,
        "bgc": BGC_FILE,
        "meteo": METEO_FILE,
        "static": STATIC_FILE,
    }

    for label, path in files.items():
        assert_file_exists(path, label=label.upper())

    datasets = {
        label: read_parquet(path, label=label.upper())
        for label, path in files.items()
    }

    return datasets


def normalize_date_column(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if "date" not in df.columns:
        raise KeyError(f"[{label}] colonne 'date' absente")
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.floor("D")
    return out


def validate_key_columns(df: pd.DataFrame, label: str, keys: list[str]) -> None:
    missing = [c for c in keys if c not in df.columns]
    if missing:
        raise KeyError(f"[{label}] colonnes clés manquantes: {missing}")


def deduplicate_on_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if df.duplicated(subset=keys).any():
        df = df.sort_values(keys).drop_duplicates(subset=keys, keep="first").copy()
    return df


def merge_prefer_left_meta(left: pd.DataFrame, right: pd.DataFrame, right_name: str) -> pd.DataFrame:
    validate_key_columns(left, "LEFT", KEY_COLS)
    validate_key_columns(right, right_name, KEY_COLS)

    left = normalize_date_column(left, "LEFT")
    right = normalize_date_column(right, right_name)

    left = deduplicate_on_keys(left, KEY_COLS)
    right = deduplicate_on_keys(right, KEY_COLS)

    overlapping_meta = [c for c in META_COLS if c in left.columns and c in right.columns]
    right_non_meta = [c for c in right.columns if c not in KEY_COLS + overlapping_meta]

    cols_to_keep = KEY_COLS + overlapping_meta + right_non_meta
    right_trim = right[cols_to_keep].copy()

    rename_map = {c: f"{c}__{right_name}" for c in overlapping_meta}
    right_trim = right_trim.rename(columns=rename_map)

    merged = left.merge(right_trim, on=KEY_COLS, how="left")

    for col in overlapping_meta:
        alt = f"{col}__{right_name}"
        if alt in merged.columns:
            if col not in merged.columns:
                merged = merged.rename(columns={alt: col})
            else:
                merged[col] = merged[col].where(merged[col].notna(), merged[alt])
                merged = merged.drop(columns=[alt])

    return merged


def merge_static_by_spot(left: pd.DataFrame, static_df: pd.DataFrame) -> pd.DataFrame:
    validate_key_columns(left, "LEFT", ["spot_id"])

    if "spot_id" not in static_df.columns:
        raise KeyError("[STATIC] colonne 'spot_id' absente")

    static_df = static_df.drop_duplicates(subset=["spot_id"]).copy()

    # On évite d'écraser les métadonnées déjà présentes dans le dataset principal
    overlapping_non_key = [c for c in static_df.columns if c in left.columns and c != "spot_id"]
    rename_map = {c: f"{c}__static" for c in overlapping_non_key}
    static_df = static_df.rename(columns=rename_map)

    merged = left.merge(static_df, on="spot_id", how="left")

    # Si une colonne existe déjà à gauche, on garde la gauche.
    # Si elle était absente, on reprend la version static.
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


def prepare_bgc(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    missing_ok = [c for c in BGC_DROP_IF_MISSING_OK if c not in out.columns]
    if missing_ok:
        log_warning(f"[BGC] colonnes absentes ignorées: {missing_ok}")
    return out


def build_final_dataset(
    target: pd.DataFrame,
    phy: pd.DataFrame,
    wav: pd.DataFrame,
    bgc: pd.DataFrame,
    meteo: pd.DataFrame,
    static: pd.DataFrame,
) -> pd.DataFrame:
    df = target.copy()
    log_shape("TARGET", df)

    df = merge_prefer_left_meta(df, phy, "phy")
    log_shape("MERGE PHY", df)

    df = merge_prefer_left_meta(df, wav, "wav")
    log_shape("MERGE WAV", df)

    df = merge_prefer_left_meta(df, bgc, "bgc")
    log_shape("MERGE BGC", df)

    df = merge_prefer_left_meta(df, meteo, "meteo")
    log_shape("MERGE METEO", df)

    df = merge_static_by_spot(df, static)
    log_shape("MERGE STATIC", df)

    df = df.sort_values(["spot_id", "date"]).reset_index(drop=True)
    return df


def main() -> None:
    ensure_dir(FINAL_DIR)

    log_kv("TARGET_FILE", TARGET_FILE)
    log_kv("PHY_FILE", PHY_FILE)
    log_kv("WAV_FILE", WAV_FILE)
    log_kv("BGC_FILE", BGC_FILE)
    log_kv("METEO_FILE", METEO_FILE)
    log_kv("STATIC_FILE", STATIC_FILE)

    datasets = load_input_datasets()

    target = normalize_date_column(datasets["target"], "TARGET")
    phy = normalize_date_column(datasets["phy"], "PHY")
    wav = normalize_date_column(datasets["wav"], "WAV")
    bgc = normalize_date_column(prepare_bgc(datasets["bgc"]), "BGC")
    meteo = normalize_date_column(datasets["meteo"], "METEO")
    static = datasets["static"].copy()

    final_df = build_final_dataset(
        target=target,
        phy=phy,
        wav=wav,
        bgc=bgc,
        meteo=meteo,
        static=static,
    )

    summary_df = build_column_summary(final_df)

    written = save_parquet_bundle(
        df=final_df,
        parquet_path=FINAL_DATASET_FILE,
        sample_path=FINAL_DATASET_SAMPLE_FILE,
        summary_df=summary_df,
        summary_path=FINAL_DATASET_SUMMARY_FILE,
        index=False,
    )

    print()
    for path in written.values():
        log_file_written(path)

    print()
    log_df_head(final_df, n=5)
    print()
    log_dataset_overview(final_df)

    print("\nNaN % top 15:")
    print(summary_df.head(15))


if __name__ == "__main__":
    main()