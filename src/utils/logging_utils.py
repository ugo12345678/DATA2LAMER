from __future__ import annotations

from pathlib import Path
import pandas as pd


def print_header(title: str, width: int = 90) -> None:
    print("\n" + "=" * width)
    print(title)
    print("=" * width)


def log_kv(key: str, value) -> None:
    print(f"{key:<14}: {value}")


def log_shape(name: str, df: pd.DataFrame) -> None:
    print(f"[{name}] shape = {df.shape}")


def log_df_head(df: pd.DataFrame, n: int = 5, title: str = "Head") -> None:
    print(f"\n{title}:")
    print(df.head(n))


def log_file_written(path: Path | str) -> None:
    print(f"✅ {Path(path)}")


def log_warning(message: str) -> None:
    print(f"[WARN] {message}")


def log_info(message: str) -> None:
    print(f"[INFO] {message}")


def log_step(message: str) -> None:
    print(f"[STEP] {message}")


def log_dataset_overview(df: pd.DataFrame, date_col: str = "date", spot_col: str = "spot_id") -> None:
    print(f"Shape: {df.shape}")
    if spot_col in df.columns:
        print(f"Nb spots: {df[spot_col].nunique()}")
    if date_col in df.columns:
        dt = pd.to_datetime(df[date_col], errors="coerce")
        print(f"Date min/max: {dt.min()} -> {dt.max()}")