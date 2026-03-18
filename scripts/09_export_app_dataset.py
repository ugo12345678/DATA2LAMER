from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from config.settings import BASE_DIR
from src.utils.io_utils import ensure_dir
from src.utils.logging_utils import log_kv, log_file_written


DEFAULT_SOURCE_CANDIDATES = [
    BASE_DIR / "data" / "processed" / "ml" / "dataset_visibility_mvp_ml_ready_2024_2026.parquet",
]

DEFAULT_OUTPUT_PATH = (
    BASE_DIR / "data" / "serving" / "dataset_visibility_app.parquet"
)


def resolve_source_path() -> Path:
    """
    Ordre de priorité :
    1. variable d'env APP_DATASET_SOURCE
    2. chemin par défaut connu
    3. dernier parquet *ml_ready*.parquet trouvé dans data/processed/ml
    """
    env_source = os.getenv("APP_DATASET_SOURCE")
    if env_source:
        path = Path(env_source)
        if path.exists():
            return path
        raise FileNotFoundError(f"APP_DATASET_SOURCE introuvable: {path}")

    for candidate in DEFAULT_SOURCE_CANDIDATES:
        if candidate.exists():
            return candidate

    ml_dir = BASE_DIR / "data" / "processed" / "ml"
    if ml_dir.exists():
        candidates = sorted(ml_dir.glob("*ml_ready*.parquet"), key=lambda p: p.stat().st_mtime)
        if candidates:
            return candidates[-1]

    raise FileNotFoundError(
        "Aucun dataset ML-ready trouvé. "
        "Vérifie que 07_prepare_ml_dataset.py a bien été exécuté."
    )


def resolve_output_path() -> Path:
    env_output = os.getenv("APP_DATASET_OUTPUT")
    if env_output:
        return Path(env_output)
    return DEFAULT_OUTPUT_PATH


def parse_selected_columns() -> list[str] | None:
    """
    Permet de limiter les colonnes exportées via:
    APP_DATASET_COLUMNS="date,spot_id,zsd,sst,wave_height"
    """
    raw = os.getenv("APP_DATASET_COLUMNS", "").strip()
    if not raw:
        return None

    cols = [c.strip() for c in raw.split(",") if c.strip()]
    return cols or None


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.floor("D")

    # downcast léger pour réduire la taille
    float_cols = out.select_dtypes(include=["float64"]).columns
    int_cols = out.select_dtypes(include=["int64"]).columns

    for col in float_cols:
        out[col] = pd.to_numeric(out[col], downcast="float")

    for col in int_cols:
        out[col] = pd.to_numeric(out[col], downcast="integer")

    # passage en category pour quelques colonnes fréquentes
    for col in ["spot_id", "spot_name", "cluster"]:
        if col in out.columns and out[col].dtype == "object":
            out[col] = out[col].astype("category")

    return out


def select_columns(df: pd.DataFrame, selected_columns: list[str] | None) -> pd.DataFrame:
    if not selected_columns:
        return df.copy()

    missing = [c for c in selected_columns if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes demandées absentes du dataset source: {missing}")

    return df[selected_columns].copy()


def build_app_dataset(
    source_path: Path,
    output_path: Path,
    selected_columns: list[str] | None = None,
    compression: str = "snappy",
) -> Path:
    log_kv("APP_DATASET_SOURCE", source_path)
    log_kv("APP_DATASET_OUTPUT", output_path)
    log_kv("APP_DATASET_COMPRESSION", compression)

    df = pd.read_parquet(source_path)

    print(f"Shape source: {df.shape}")
    print(f"Colonnes source: {len(df.columns)}")

    app_df = select_columns(df, selected_columns)
    app_df = optimize_dataframe(app_df)
    app_df = app_df.sort_values(
        [c for c in ["spot_id", "date"] if c in app_df.columns]
    ).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    app_df.to_parquet(output_path, index=False, compression=compression)

    print(f"Shape export app: {app_df.shape}")
    print(f"Colonnes export app: {len(app_df.columns)}")

    if "spot_id" in app_df.columns:
        print(f"Nb spots export app: {app_df['spot_id'].nunique()}")

    if "date" in app_df.columns:
        dt = pd.to_datetime(app_df["date"], errors="coerce")
        print(f"Date min/max export app: {dt.min()} -> {dt.max()}")

    log_file_written(output_path)
    return output_path


def main() -> None:
    ensure_dir(BASE_DIR / "data" / "serving")

    source_path = resolve_source_path()
    output_path = resolve_output_path()
    selected_columns = parse_selected_columns()
    compression = os.getenv("APP_DATASET_COMPRESSION", "snappy")

    if selected_columns:
        print(f"Colonnes sélectionnées ({len(selected_columns)}): {selected_columns}")
    else:
        print("Aucune sélection de colonnes spécifique : export complet du dataset ML-ready.")

    build_app_dataset(
        source_path=source_path,
        output_path=output_path,
        selected_columns=selected_columns,
        compression=compression,
    )


if __name__ == "__main__":
    main()