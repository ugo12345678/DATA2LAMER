from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import copernicusmarine
import pandas as pd

from config.settings import (
    BASE_DIR,
    SCRIPTS_DIR,
    RAW_DIR,
    TARGET_DIR,
    SPOTS_SOURCE_FILE,
    SPOTS_VALID_FILE,
    SPOT_VALID_COUNTS_FILE,
    RUN_METADATA_FILE,
    TARGET_FILE,
    MIN_VALID_DAYS_PER_SPOT,
)
from config.pipeline import (
    YEAR_FILTER,
    DOWNLOAD_DATASETS,
    MIN_EXPECTED_NC_FILES,
    SCRIPT_OUTPUTS,
    BUILD_SCRIPTS,
    POST_BUILD_SCRIPTS,
)

from src.utils.io_utils import (
    ensure_dir,
    assert_file_exists,
    read_csv,
    read_parquet,
    write_csv,
    write_json,
    write_parquet,
)
from src.utils.logging_utils import print_header, log_file_written


def count_nc_files(folder: Path) -> int:
    if not folder.exists():
        return 0
    return sum(1 for _ in folder.rglob("*.nc"))


def should_download(subdir: str) -> bool:
    folder = RAW_DIR / subdir
    count = count_nc_files(folder)
    expected = MIN_EXPECTED_NC_FILES.get(subdir, 1)
    print(f"[CHECK RAW] {subdir}: {count} fichier(s) .nc trouvé(s)")
    return count < expected


def download_dataset(dataset_id: str, output_directory: Path) -> None:
    ensure_dir(output_directory)
    print(f"\n=== Download {dataset_id} -> {output_directory} ===")
    copernicusmarine.get(
        dataset_id=dataset_id,
        output_directory=str(output_directory),
        filter=YEAR_FILTER,
        overwrite=False,
        disable_progress_bar=False,
    )


def ensure_all_downloads() -> list[dict]:
    downloaded: list[dict] = []

    for cfg in DOWNLOAD_DATASETS:
        subdir = cfg["subdir"]
        out_dir = RAW_DIR / subdir

        if should_download(subdir):
            download_dataset(cfg["dataset_id"], out_dir)
            downloaded.append(
                {
                    "name": cfg["name"],
                    "dataset_id": cfg["dataset_id"],
                    "subdir": subdir,
                    "output_directory": str(out_dir),
                }
            )
        else:
            print(f"[SKIP DOWNLOAD] {subdir} déjà présent")

    return downloaded


def outputs_exist(script_name: str) -> bool:
    outputs = SCRIPT_OUTPUTS.get(script_name, [])
    if not outputs:
        return False
    return all(path.exists() for path in outputs)


def run_script(script_name: str, extra_env: dict[str, str] | None = None) -> None:
    script_path = SCRIPTS_DIR / script_name
    assert_file_exists(script_path, label="Script")

    env = os.environ.copy()

    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{BASE_DIR}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = str(BASE_DIR)

    if extra_env:
        env.update(extra_env)

    print_header(f"RUN SCRIPT: {script_name}")
    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        env=env,
        check=True,
    )


def run_script_if_needed(
    script_name: str,
    extra_env: dict[str, str] | None = None,
    force: bool = False,
) -> None:
    if not force and outputs_exist(script_name):
        print(f"[SKIP BUILD] {script_name} -> outputs déjà présents")
        return
    run_script(script_name, extra_env=extra_env)


def find_latest_target_parquet() -> Path:
    candidates = sorted(
        [
            p
            for p in TARGET_DIR.glob("target_zsd_daily_by_spot_*.parquet")
            if "sample" not in p.name and "filtered" not in p.name
        ]
    )
    if not candidates:
        raise FileNotFoundError(
            f"Aucun fichier target_zsd_daily_by_spot_*.parquet trouvé dans {TARGET_DIR}"
        )
    return candidates[-1]


def filter_valid_spots(
    target_parquet: Path,
    spots_source_csv: Path = SPOTS_SOURCE_FILE,
    min_valid_days: int = MIN_VALID_DAYS_PER_SPOT,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    target_df = read_parquet(target_parquet, label="Parquet target")
    spots_df = read_csv(spots_source_csv, label="CSV spots source")

    if "zsd" not in target_df.columns:
        raise ValueError(f"Le parquet cible {target_parquet} ne contient pas de colonne 'zsd'")

    target_df["date"] = pd.to_datetime(target_df["date"], errors="coerce").dt.floor("D")
    target_df = target_df.dropna(subset=["date", "zsd"]).copy()

    counts = (
        target_df.groupby("spot_id")["date"]
        .nunique()
        .rename("valid_days")
        .reset_index()
        .sort_values(["valid_days", "spot_id"], ascending=[True, True])
        .reset_index(drop=True)
    )

    valid_spot_ids = counts.loc[counts["valid_days"] >= min_valid_days, "spot_id"].tolist()

    filtered_spots = (
        spots_df[spots_df["spot_id"].isin(valid_spot_ids)]
        .sort_values("spot_id")
        .reset_index(drop=True)
        .copy()
    )

    filtered_target = (
        target_df[target_df["spot_id"].isin(valid_spot_ids)]
        .sort_values(["spot_id", "date"])
        .reset_index(drop=True)
        .copy()
    )

    return filtered_spots, filtered_target, counts


def save_filtered_outputs(
    filtered_spots: pd.DataFrame,
    filtered_target: pd.DataFrame,
    counts: pd.DataFrame,
) -> dict[str, str]:
    filtered_sample_file = TARGET_DIR / "target_zsd_daily_by_spot_filtered_sample.parquet"

    write_csv(filtered_spots, SPOTS_VALID_FILE, index=False)
    write_parquet(filtered_target, TARGET_FILE, index=False)

    sample = filtered_target.groupby("spot_id", group_keys=False).head(60).reset_index(drop=True)
    write_parquet(sample, filtered_sample_file, index=False)

    write_csv(counts, SPOT_VALID_COUNTS_FILE, index=False)

    log_file_written(SPOTS_VALID_FILE)
    log_file_written(TARGET_FILE)
    log_file_written(filtered_sample_file)
    log_file_written(SPOT_VALID_COUNTS_FILE)

    return {
        "spots_valid_csv": str(SPOTS_VALID_FILE),
        "target_filtered_parquet": str(TARGET_FILE),
        "target_filtered_sample_parquet": str(filtered_sample_file),
        "spot_counts_csv": str(SPOT_VALID_COUNTS_FILE),
    }


def write_run_metadata(downloaded: list[dict], saved_outputs: dict[str, str]) -> Path:
    metadata = {
        "min_valid_days_zsd": MIN_VALID_DAYS_PER_SPOT,
        "downloaded_now": downloaded,
        "saved_outputs": saved_outputs,
        "build_scripts": BUILD_SCRIPTS,
        "post_build_scripts": POST_BUILD_SCRIPTS,
    }
    write_json(metadata, RUN_METADATA_FILE, indent=2, ensure_ascii=False)
    log_file_written(RUN_METADATA_FILE)
    return RUN_METADATA_FILE