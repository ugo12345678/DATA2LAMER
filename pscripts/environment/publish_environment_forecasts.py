from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pscripts.environment.consolidation import consolidate_source_values
from pscripts.environment.r2_storage import R2SourceValueArchive
from pscripts.environment.repositories import Vu2LamerForecastRepository


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


def main() -> None:
    print("=== ENVIRONMENT FORECAST PUBLISH FROM R2 ===")

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
    deleted = app_repo.delete_expired()
    upserted = app_repo.upsert(consolidated_rows)

    print(f"[OK] VU2LAMER environment rows upserted: {upserted}")
    print(f"[OK] VU2LAMER expired rows deleted: {deleted}")
    print(json.dumps(consolidated_rows[:2], indent=2, default=str))


if __name__ == "__main__":
    main()
