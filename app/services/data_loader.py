from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


DEFAULT_DATASET_CANDIDATES = [
    Path("data/processed/ml/dataset_visibility_mvp_ml_ready_2024_2026.parquet"),
    Path("data/processed/final/dataset_visibility_mvp_temporal_2024_2026.parquet"),
    Path("data/processed/final/dataset_visibility_mvp_2024_2026.parquet"),
]


@dataclass(frozen=True)
class DatasetInfo:
    path: Path
    shape: tuple[int, int]
    n_spots: int
    date_min: pd.Timestamp | None
    date_max: pd.Timestamp | None
    numeric_cols: list[str]
    categorical_cols: list[str]



def resolve_dataset_path() -> Path:
    env_path = os.environ.get("STREAMLIT_DATASET_PATH")
    if env_path:
        path = Path(env_path)
        if path.exists():
            return path

    for candidate in DEFAULT_DATASET_CANDIDATES:
        if candidate.exists():
            return candidate

    searched = "\n - ".join(str(p) for p in DEFAULT_DATASET_CANDIDATES)
    raise FileNotFoundError(
        "Aucun dataset parquet trouvé.\n"
        "Définis STREAMLIT_DATASET_PATH ou place un fichier ici :\n"
        f" - {searched}"
    )



def load_dataset(path: str | Path | None = None) -> pd.DataFrame:
    dataset_path = Path(path) if path else resolve_dataset_path()
    df = pd.read_parquet(dataset_path)

    if "date" in df.columns:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df



def build_dataset_info(df: pd.DataFrame, path: str | Path | None = None) -> DatasetInfo:
    dataset_path = Path(path) if path else resolve_dataset_path()

    numeric_cols = sorted(df.select_dtypes(include=["number", "bool"]).columns.tolist())
    categorical_cols = sorted([c for c in df.columns if c not in numeric_cols])

    date_min = None
    date_max = None
    if "date" in df.columns:
        date_min = pd.to_datetime(df["date"], errors="coerce").min()
        date_max = pd.to_datetime(df["date"], errors="coerce").max()

    n_spots = int(df["spot_id"].nunique()) if "spot_id" in df.columns else 0

    return DatasetInfo(
        path=dataset_path,
        shape=df.shape,
        n_spots=n_spots,
        date_min=date_min,
        date_max=date_max,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
    )



def summarize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    summary = pd.DataFrame(
        {
            "column": df.columns,
            "missing_count": df.isna().sum().values,
            "missing_pct": (df.isna().mean().values * 100).round(2),
            "dtype": df.dtypes.astype(str).values,
        }
    )
    return summary.sort_values(["missing_count", "column"], ascending=[False, True]).reset_index(drop=True)



def filter_dataset(
    df: pd.DataFrame,
    selected_spots: list[str] | None = None,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    out = df.copy()

    if selected_spots and "spot_id" in out.columns:
        out = out[out["spot_id"].astype(str).isin([str(s) for s in selected_spots])]

    if "date" in out.columns:
        if start_date is not None:
            out = out[out["date"] >= pd.Timestamp(start_date)]
        if end_date is not None:
            out = out[out["date"] <= pd.Timestamp(end_date)]

    return out.reset_index(drop=True)
