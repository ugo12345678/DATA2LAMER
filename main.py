from __future__ import annotations

from config.settings import (
    BASE_DIR,
    RAW_DIR,
    TARGET_DIR,
    SPOTS_SOURCE_FILE,
    SPOTS_VALID_FILE,
    DIRS_TO_CREATE,
    MIN_VALID_DAYS_PER_SPOT,
)
from config.pipeline import BUILD_SCRIPTS, POST_BUILD_SCRIPTS

from src.utils.io_utils import ensure_dir, read_csv
from src.utils.logging_utils import log_kv, log_file_written
from src.pipeline.orchestrator import (
    ensure_all_downloads,
    run_script_if_needed,
    find_latest_target_parquet,
    filter_valid_spots,
    save_filtered_outputs,
    write_run_metadata,
)


def main() -> None:
    for directory in DIRS_TO_CREATE:
        ensure_dir(directory)

    log_kv("BASE_DIR", BASE_DIR)
    log_kv("RAW_DIR", RAW_DIR)
    log_kv("TARGET_DIR", TARGET_DIR)

    downloaded = ensure_all_downloads()

    run_script_if_needed(
        "01_build_target_zsd_pipeline.py",
        extra_env={"SPOTS_FILE_OVERRIDE": str(SPOTS_SOURCE_FILE)},
    )

    target_parquet = find_latest_target_parquet()
    print(f"\nParquet target détecté : {target_parquet}")

    filtered_spots, filtered_target, counts = filter_valid_spots(
        target_parquet=target_parquet,
        spots_source_csv=SPOTS_SOURCE_FILE,
        min_valid_days=MIN_VALID_DAYS_PER_SPOT,
    )

    spots_source_df = read_csv(SPOTS_SOURCE_FILE, label="CSV spots source")
    print(f"\nSpots source: {spots_source_df['spot_id'].nunique()}")
    print(f"Spots valides (>= {MIN_VALID_DAYS_PER_SPOT} jours): {filtered_spots['spot_id'].nunique()}")

    saved_outputs = save_filtered_outputs(
        filtered_spots=filtered_spots,
        filtered_target=filtered_target,
        counts=counts,
    )

    extra_env = {"SPOTS_FILE_OVERRIDE": str(SPOTS_VALID_FILE)}

    for script_name in BUILD_SCRIPTS[1:]:
        run_script_if_needed(script_name, extra_env=extra_env)

    for script_name in POST_BUILD_SCRIPTS:
        run_script_if_needed(script_name)

    write_run_metadata(downloaded=downloaded, saved_outputs=saved_outputs)

    print("\n✅ Pipeline terminé")


if __name__ == "__main__":
    main()