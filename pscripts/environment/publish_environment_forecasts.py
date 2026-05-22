from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pscripts.environment.consolidation import consolidate_source_values
from pscripts.environment.r2_storage import R2SourceValueArchive, R2TrainingDatasetArchive
from pscripts.environment.repositories import Vu2LamerDiveTrainingDatasetRepository, Vu2LamerForecastRepository


def _selected_sources() -> set[str] | None:
    sources = {
        item.strip()
        for item in os.environ.get("FORECAST_SOURCES", "").split(",")
        if item.strip()
    }
    return sources or None


def _selected_run_time() -> datetime | None:
    run_date = os.environ.get("R2_SYNC_RUN_DATE")
    run_hour = os.environ.get("R2_SYNC_RUN_HOUR")
    if not run_date and not run_hour:
        return None
    if not run_date or not run_hour:
        raise ValueError("R2_SYNC_RUN_DATE and R2_SYNC_RUN_HOUR must be set together.")
    return datetime.fromisoformat(f"{run_date}T{int(run_hour):02d}:00:00+00:00")


def _fetch_training_dataset_rows() -> list[dict]:
    if os.environ.get("TRAINING_DATASET_EXPORT_ENABLED", "true").lower() not in {"1", "true", "yes"}:
        return []
    try:
        return Vu2LamerDiveTrainingDatasetRepository().fetch_rows()
    except Exception as exc:
        if os.environ.get("TRAINING_DATASET_EXPORT_REQUIRED", "false").lower() in {"1", "true", "yes"}:
            raise
        print(f"[WARN] R2 training dataset read skipped: {exc}")
        return []


def _publish_training_dataset(run_time: datetime, rows: list[dict]) -> None:
    if os.environ.get("TRAINING_DATASET_EXPORT_ENABLED", "true").lower() not in {"1", "true", "yes"}:
        return
    if not rows:
        print("[INFO] R2 training dataset skipped: no rows to merge.")
        return

    try:
        training_archive = R2TrainingDatasetArchive.from_env()
        result = training_archive.merge_and_write_rows(
            run_time=run_time,
            rows=rows,
            key_field=os.environ.get("TRAINING_DATASET_DEDUP_KEY", "outing_id"),
        )
        if result["latest_key"]:
            print(
                "[OK] R2 training dataset merged: "
                f"{result['latest_key']} ({result['rows_count']} total rows, "
                f"{result['delta_count']} rows from this publish)"
            )
    except Exception as exc:
        if os.environ.get("TRAINING_DATASET_EXPORT_REQUIRED", "false").lower() in {"1", "true", "yes"}:
            raise
        print(f"[WARN] R2 training dataset write skipped: {exc}")


def main() -> None:
    print("=== ENVIRONMENT FORECAST PUBLISH FROM R2 ===")
    pipeline_started_at = datetime.now(timezone.utc)

    r2_archive = R2SourceValueArchive.from_env()
    if not r2_archive.available:
        raise RuntimeError(
            "R2 raw source archive is not configured. Missing settings: "
            + ", ".join(r2_archive.missing_settings())
        )

    source_codes = _selected_sources()
    selected_run_time = _selected_run_time()
    if selected_run_time is None:
        lookback_hours = int(os.environ.get("R2_SYNC_LOOKBACK_HOURS", "12"))
        run_time, keys = r2_archive.latest_source_value_keys(
            lookback_hours=lookback_hours,
            source_codes=source_codes,
        )
    else:
        run_time = selected_run_time.astimezone(timezone.utc)
        keys = r2_archive.list_source_value_keys(run_time=run_time, source_codes=source_codes)

    if not run_time or not keys:
        raise RuntimeError("No R2 source value archives found for publication.")

    print(f"[INFO] Publishing R2 run: {run_time.isoformat()} ({len(keys)} files)")
    values = []
    for key in keys:
        source_values = r2_archive.read_source_values(key)
        print(f"[OK] R2 read: {key} ({len(source_values)} source values)")
        values.extend(source_values)

    if not values:
        raise RuntimeError("No source values were read from R2.")

    consolidated_rows = consolidate_source_values(values, run_time)
    if not consolidated_rows:
        raise RuntimeError("No consolidated forecast rows were produced.")

    app_repo = Vu2LamerForecastRepository()
    dataset_rows_before_purge = _fetch_training_dataset_rows()
    upserted = app_repo.upsert(consolidated_rows)
    dataset_rows_after_publish = _fetch_training_dataset_rows()
    _publish_training_dataset(run_time, [*dataset_rows_before_purge, *dataset_rows_after_publish])
    deleted = app_repo.delete_expired(cutoff=pipeline_started_at)

    print(f"[OK] VU2LAMER environment rows upserted: {upserted}")
    print(f"[OK] VU2LAMER expired rows deleted: {deleted}")
    print(json.dumps(consolidated_rows[:2], indent=2, default=str))


if __name__ == "__main__":
    main()
