from __future__ import annotations

from pathlib import Path
import json
import pandas as pd


def ensure_dir(path: Path | str) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_parent_dir(path: Path | str) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def assert_file_exists(path: Path | str, label: str | None = None) -> Path:
    path = Path(path)
    if not path.exists():
        prefix = f"{label} " if label else ""
        raise FileNotFoundError(f"{prefix}introuvable: {path}")
    return path


def read_parquet(path: Path | str, label: str | None = None, **kwargs) -> pd.DataFrame:
    path = assert_file_exists(path, label=label)
    return pd.read_parquet(path, **kwargs)


def write_parquet(df: pd.DataFrame, path: Path | str, index: bool = False) -> Path:
    path = ensure_parent_dir(path)
    df.to_parquet(path, index=index)
    return Path(path)


def read_csv(path: Path | str, label: str | None = None, **kwargs) -> pd.DataFrame:
    path = assert_file_exists(path, label=label)
    return pd.read_csv(path, **kwargs)


def write_csv(df: pd.DataFrame, path: Path | str, index: bool = False, **kwargs) -> Path:
    path = ensure_parent_dir(path)
    df.to_csv(path, index=index, **kwargs)
    return Path(path)


def read_json(path: Path | str, label: str | None = None):
    path = assert_file_exists(path, label=label)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(data, path: Path | str, indent: int = 2, ensure_ascii: bool = False) -> Path:
    path = ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii, default=str)
    return Path(path)


def write_text(text: str, path: Path | str) -> Path:
    path = ensure_parent_dir(path)
    Path(path).write_text(text, encoding="utf-8")
    return Path(path)


def dataframe_sample(df: pd.DataFrame, n: int = 200) -> pd.DataFrame:
    return df.head(min(n, len(df))).copy()


def save_parquet_bundle(
    df: pd.DataFrame,
    parquet_path: Path | str,
    sample_path: Path | str | None = None,
    summary_df: pd.DataFrame | None = None,
    summary_path: Path | str | None = None,
    index: bool = False,
) -> dict[str, Path]:
    written: dict[str, Path] = {}
    written["parquet"] = write_parquet(df, parquet_path, index=index)

    if sample_path is not None:
        written["sample"] = write_parquet(dataframe_sample(df), sample_path, index=index)

    if summary_df is not None and summary_path is not None:
        written["summary"] = write_csv(summary_df, summary_path, index=False)

    return written