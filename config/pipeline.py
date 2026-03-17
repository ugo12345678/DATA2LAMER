from __future__ import annotations

from config.settings import (
    TARGET_DIR,
    FEATURES_DIR,
    FINAL_DATASET_FILE,
    FINAL_DATASET_SAMPLE_FILE,
    FINAL_DATASET_SUMMARY_FILE,
    ML_READY_FILE,
    ML_READY_SAMPLE_FILE,
    ML_READY_SUMMARY_FILE,
    ML_READY_SPOT_COUNTS_FILE,
    MODEL_FILE,
    METRICS_FILE,
    FEATURE_IMPORTANCE_FILE,
    PREDICTIONS_FILE,
)

YEARS = [2024, 2025, 2026]
YEAR_FILTER = "*"
YEARS_LABEL = f"{min(YEARS)}_{max(YEARS)}"

MIN_EXPECTED_NC_FILES = {
    "zsd": 1,
    "phy": 1,
    "wav": 1,
    "bgc": 1,
}

DOWNLOAD_DATASETS = [
    {
        "name": "zsd",
        "dataset_id": "cmems_obs-oc_atl_bgc-transp_my_l3-multi-1km_P1D",
        "subdir": "zsd",
    },
    {
        "name": "phy",
        "dataset_id": "cmems_mod_ibi_phy_anfc_0.027deg-3D_P1D-m",
        "subdir": "phy",
    },
    {
        "name": "wav",
        "dataset_id": "cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i",
        "subdir": "wav",
    },
    {
        "name": "bgc",
        "dataset_id": "cmems_mod_ibi_bgc_anfc_0.027deg-3D_P1D-m",
        "subdir": "bgc",
    },
]

BUILD_SCRIPTS = [
    "01_build_target_zsd_pipeline.py",
    "02_build_phy_pipeline.py",
    "03_build_wav_pipeline.py",
    "04_build_bgc_pipeline.py",
    "05_build_meteo_pipeline.py",
    "05b_build_static_pipeline.py",
]

POST_BUILD_SCRIPTS = [
    "06_join_features.py",
    "07_prepare_ml_dataset.py",
    # "08_train_baseline_model.py",
]

SCRIPT_OUTPUTS = {
    "01_build_target_zsd_pipeline.py": [
        TARGET_DIR / f"target_zsd_daily_by_spot_{YEARS_LABEL}.parquet",
        TARGET_DIR / "target_zsd_daily_by_spot_sample.parquet",
    ],
    "02_build_phy_pipeline.py": [
        FEATURES_DIR / f"features_phy_daily_by_spot_{YEARS_LABEL}.parquet",
        FEATURES_DIR / "features_phy_daily_by_spot_sample.parquet",
    ],
    "03_build_wav_pipeline.py": [
        FEATURES_DIR / f"features_wav_daily_by_spot_{YEARS_LABEL}.parquet",
        FEATURES_DIR / "features_wav_daily_by_spot_sample.parquet",
    ],
    "04_build_bgc_pipeline.py": [
        FEATURES_DIR / f"features_bgc_daily_by_spot_{YEARS_LABEL}.parquet",
        FEATURES_DIR / "features_bgc_daily_by_spot_sample.parquet",
    ],
    "05_build_meteo_pipeline.py": [
        FEATURES_DIR / f"features_meteo_daily_by_spot_{YEARS_LABEL}.parquet",
        FEATURES_DIR / "features_meteo_daily_by_spot_sample.parquet",
    ],
    "05b_build_static_pipeline.py": [
        FEATURES_DIR / "features_static_by_spot.parquet",
        FEATURES_DIR / "features_static_by_spot_sample.parquet",
        FEATURES_DIR / "features_static_by_spot_summary.csv",
    ],
    "06_join_features.py": [
        FINAL_DATASET_FILE,
        FINAL_DATASET_SAMPLE_FILE,
        FINAL_DATASET_SUMMARY_FILE,
    ],
    "07_prepare_ml_dataset.py": [
        ML_READY_FILE,
        ML_READY_SAMPLE_FILE,
        ML_READY_SUMMARY_FILE,
        ML_READY_SPOT_COUNTS_FILE,
    ],
    "08_train_baseline_model.py": [
        MODEL_FILE,
        METRICS_FILE,
        FEATURE_IMPORTANCE_FILE,
        PREDICTIONS_FILE,
    ],
}