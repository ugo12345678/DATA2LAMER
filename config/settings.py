from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# =========================
# Directories
# =========================
DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
SCRIPTS_DIR = BASE_DIR / "scripts"
TARGET_DIR = PROCESSED_DIR / "target"
FEATURES_DIR = PROCESSED_DIR / "features"
FINAL_DIR = PROCESSED_DIR / "final"
ML_DIR = PROCESSED_DIR / "ml"

MODELS_DIR = DATA_DIR / "models"
REPORTS_DIR = DATA_DIR / "reports"
PREDICTIONS_DIR = DATA_DIR / "predictions"
CONFIG_DATA_DIR = DATA_DIR / "config"

RAW_STATIC_DIR = RAW_DIR / "static"
BATHY_DIR = RAW_STATIC_DIR / "bathymetry"
COASTLINE_DIR = RAW_STATIC_DIR / "coastline"

DIRS_TO_CREATE = [
    RAW_DIR,
    PROCESSED_DIR,
    TARGET_DIR,
    FEATURES_DIR,
    FINAL_DIR,
    ML_DIR,
    MODELS_DIR,
    REPORTS_DIR,
    PREDICTIONS_DIR,
    CONFIG_DATA_DIR,
    RAW_STATIC_DIR,
    BATHY_DIR,
    COASTLINE_DIR,
]

# =========================
# Input files
# =========================
SPOTS_SOURCE_FILE = BASE_DIR / "spots_bretagne_mvp_50.csv"
SPOTS_VALID_FILE = CONFIG_DATA_DIR / "spots_bretagne_mvp_valides.csv"
SPOT_VALID_COUNTS_FILE = CONFIG_DATA_DIR / "spot_valid_counts.csv"
RUN_METADATA_FILE = CONFIG_DATA_DIR / "run_metadata.json"

# =========================
# Business parameters
# =========================
MIN_VALID_DAYS_PER_SPOT = 200

# =========================
# Target outputs
# =========================
TARGET_FILE = TARGET_DIR / "target_zsd_daily_by_spot_filtered.parquet"
TARGET_SAMPLE_FILE = TARGET_DIR / "target_zsd_daily_by_spot_filtered_sample.parquet"

PHY_FILE = FEATURES_DIR / "features_phy_daily_by_spot_2024_2026.parquet"
WAV_FILE = FEATURES_DIR / "features_wav_daily_by_spot_2024_2026.parquet"
BGC_FILE = FEATURES_DIR / "features_bgc_daily_by_spot_2024_2026.parquet"
METEO_FILE = FEATURES_DIR / "features_meteo_daily_by_spot_2024_2026.parquet"
STATIC_FILE = FEATURES_DIR / "features_static_by_spot.parquet"
STATIC_SAMPLE_FILE = FEATURES_DIR / "features_static_by_spot_sample.parquet"
STATIC_SUMMARY_FILE = FEATURES_DIR / "features_static_by_spot_summary.csv"

FINAL_DATASET_FILE = FINAL_DIR / "dataset_visibility_mvp_2024_2026.parquet"
FINAL_DATASET_SAMPLE_FILE = FINAL_DIR / "dataset_visibility_mvp_sample.parquet"
FINAL_DATASET_SUMMARY_FILE = FINAL_DIR / "dataset_visibility_mvp_summary.csv"

ML_READY_FILE = ML_DIR / "dataset_visibility_mvp_ml_ready_2024_2026.parquet"
ML_READY_SAMPLE_FILE = ML_DIR / "dataset_visibility_mvp_ml_ready_sample.parquet"
ML_READY_SUMMARY_FILE = ML_DIR / "dataset_visibility_mvp_ml_ready_summary.csv"
ML_READY_SPOT_COUNTS_FILE = ML_DIR / "dataset_visibility_mvp_ml_ready_spot_counts.csv"

MODEL_FILE = MODELS_DIR / "baseline_visibility_model_2024_2026.joblib"
METRICS_FILE = REPORTS_DIR / "baseline_metrics_2024_2026.json"
FEATURE_IMPORTANCE_FILE = REPORTS_DIR / "baseline_feature_importance_2024_2026.csv"
PREDICTIONS_FILE = PREDICTIONS_DIR / "baseline_predictions_2024_2026.parquet"