from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from pscripts.environment.consolidation import consolidate_source_values
from pscripts.environment.entities import SourceValue
from pscripts.environment.repositories import Data2LamerForecastRepository, Vu2LamerForecastRepository
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.sources.cmems import CmemsBgcSource, CmemsPhySource, CmemsWavSource, cmems_enabled
from pscripts.environment.sources.metno import MetNoLocationForecastSource
from pscripts.environment.sources.open_meteo import (
    OpenMeteoDwdIconSource,
    OpenMeteoGfsSource,
    OpenMeteoMarineDwdEwamSource,
    OpenMeteoMarineDwdGwamSource,
    OpenMeteoMarineGfsWaveSource,
    OpenMeteoMarineMeteoFranceCurrentsSource,
    OpenMeteoMarineMeteoFranceSstSource,
    OpenMeteoMarineMeteoFranceWaveSource,
    OpenMeteoMarineSource,
    OpenMeteoMeteoFranceSource,
    OpenMeteoWeatherSource,
)
from pscripts.environment.timeutils import utc_now_hour
from pscripts.spots import load_spots


def build_sources() -> list[ForecastSource]:
    sources: list[ForecastSource] = [
        OpenMeteoWeatherSource(),
        OpenMeteoMeteoFranceSource(),
        OpenMeteoDwdIconSource(),
        OpenMeteoGfsSource(),
        OpenMeteoMarineSource(),
        OpenMeteoMarineMeteoFranceWaveSource(),
        OpenMeteoMarineMeteoFranceCurrentsSource(),
        OpenMeteoMarineMeteoFranceSstSource(),
        OpenMeteoMarineDwdEwamSource(),
        OpenMeteoMarineDwdGwamSource(),
        OpenMeteoMarineGfsWaveSource(),
    ]

    if os.environ.get("ENABLE_METNO", "true").lower() in {"1", "true", "yes"}:
        sources.append(MetNoLocationForecastSource())

    if os.environ.get("ENABLE_CMEMS", "true").lower() in {"1", "true", "yes"} and cmems_enabled():
        sources.extend([CmemsWavSource(), CmemsPhySource(), CmemsBgcSource()])
    else:
        print("[INFO] CMEMS disabled or credentials missing; using free no-credential sources only.")

    disabled = {
        item.strip()
        for item in os.environ.get("DISABLED_FORECAST_SOURCES", "").split(",")
        if item.strip()
    }
    return [source for source in sources if source.config.code not in disabled]


def fetch_source_values(
    sources: list[ForecastSource],
    data_repo: Data2LamerForecastRepository,
) -> tuple[list[SourceValue], datetime]:
    spots = load_spots()
    run_time = utc_now_hour()
    forecast_days = int(os.environ.get("FORECAST_DAYS", "7"))
    window_end = run_time + timedelta(days=forecast_days)
    all_values: list[SourceValue] = []

    data_repo.ensure_sources([source.config for source in sources])
    max_workers = int(os.environ.get("FORECAST_THREAD_WORKERS", "4"))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        run_ids = {}
        for source in sources:
            run_id = data_repo.create_run(source.config, run_time, run_time, window_end)
            run_ids[source.config.code] = run_id
            futures[executor.submit(source.fetch, spots, run_time)] = source

        for future in as_completed(futures):
            source = futures[future]
            run_id = run_ids[source.config.code]
            try:
                values = future.result()
                for value in values:
                    value.run_id = run_id

                inserted = data_repo.insert_source_values(values)
                data_repo.upsert_grid_points(values)
                data_repo.finish_run(run_id, "success", rows_count=len(values))
                print(f"[OK] {source.config.code}: {len(values)} source values ({inserted} stored in DATA2LAMER)")
                all_values.extend(values)
            except Exception as exc:
                data_repo.finish_run(run_id, "failed", rows_count=0, error=str(exc))
                print(f"[ERROR] {source.config.code}: {exc}")

    return all_values, run_time


def main() -> None:
    print("=== ENVIRONMENT FORECAST SYNC ===")
    sources = build_sources()
    print("Sources:", ", ".join(source.config.code for source in sources))

    data_repo = Data2LamerForecastRepository()
    if not data_repo.available:
        print("[INFO] DATA2LAMER is not configured; raw source values will not be stored.")

    values, run_time = fetch_source_values(sources, data_repo)
    if not values:
        raise RuntimeError("No forecast source values were fetched.")

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
