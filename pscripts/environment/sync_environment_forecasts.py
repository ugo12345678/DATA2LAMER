from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from pscripts.environment.consolidation import consolidate_source_values
from pscripts.environment.entities import SourceValue
from pscripts.environment.metrics import METRICS
from pscripts.environment.r2_storage import R2SourceValueArchive
from pscripts.environment.repositories import Data2LamerForecastRepository, Vu2LamerForecastRepository
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.sources.cmems import CmemsBgcSource, CmemsPhySource, CmemsWavSource, cmems_enabled
from pscripts.environment.sources.shom import ShomTideCoefficientSource, shom_tide_enabled
from pscripts.environment.sources.metno import MetNoLocationForecastSource
from pscripts.environment.sources.open_meteo import (
    OpenMeteoDwdIconSource,
    OpenMeteoGfsSource,
    OpenMeteoMarineDwdEwamSource,
    OpenMeteoMarineDwdGwamSource,
    OpenMeteoMarineGfsWaveSource,
    OpenMeteoMarineMeteoFranceCurrentsSource,
    OpenMeteoMarineMeteoFranceWaveSource,
    OpenMeteoMarineSource,
    OpenMeteoMeteoFranceSource,
    OpenMeteoWeatherSource,
)
from pscripts.environment.timeutils import utc_now_hour
from pscripts.spots import load_spots


ENVIRONMENT_FORECAST_BASE_COLUMNS = [
    "spot_id",
    "valid_time",
    "target_date",
    "forecast_run_at",
    "forecast_horizon_hours",
    "source_count",
    "sources",
    "provenance",
]


def _env_enabled(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).lower() in {"1", "true", "yes"}


def environment_forecast_columns() -> list[str]:
    metric_columns = []
    for spec in METRICS.values():
        if spec.column not in metric_columns:
            metric_columns.append(spec.column)
    return [*ENVIRONMENT_FORECAST_BASE_COLUMNS, *metric_columns]


def environment_forecast_column_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts = {column: 0 for column in environment_forecast_columns()}
    for row in rows:
        for column in counts:
            if row.get(column) is not None:
                counts[column] += 1
    return counts


def log_environment_forecast_column_counts(rows: list[dict[str, object]]) -> None:
    counts = environment_forecast_column_counts(rows)
    total_rows = len(rows)
    print(f"[INFO] environment_forecasts column coverage: rows={total_rows}")
    for column, count in counts.items():
        ratio = (count / total_rows * 100.0) if total_rows else 0.0
        print(f"[INFO]   {column}: {count}/{total_rows} ({ratio:.1f}%)")


def source_value_metric_counts(values: list[SourceValue]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for value in values:
        if value.value is None:
            continue
        source_counts = counts.setdefault(value.source_code, {})
        source_counts[value.metric] = source_counts.get(value.metric, 0) + 1
    return {
        source_code: dict(sorted(metric_counts.items()))
        for source_code, metric_counts in sorted(counts.items())
    }


def log_source_value_metric_counts(values: list[SourceValue]) -> None:
    counts = source_value_metric_counts(values)
    total_values = sum(sum(metric_counts.values()) for metric_counts in counts.values())
    print(f"[INFO] source value coverage: values={total_values} sources={len(counts)}")
    for source_code, metric_counts in counts.items():
        source_total = sum(metric_counts.values())
        metrics = ", ".join(f"{metric}={count}" for metric, count in metric_counts.items())
        print(f"[INFO]   {source_code}: {source_total} values ({metrics})")


def build_sources() -> list[ForecastSource]:
    sources: list[ForecastSource] = [
        OpenMeteoWeatherSource(),
        OpenMeteoMeteoFranceSource(),
        OpenMeteoDwdIconSource(),
        OpenMeteoGfsSource(),
        OpenMeteoMarineSource(),
        OpenMeteoMarineMeteoFranceWaveSource(),
        OpenMeteoMarineMeteoFranceCurrentsSource(),
        OpenMeteoMarineDwdEwamSource(),
        OpenMeteoMarineDwdGwamSource(),
        OpenMeteoMarineGfsWaveSource(),
    ]

    if os.environ.get("ENABLE_METNO", "true").lower() in {"1", "true", "yes"}:
        sources.append(MetNoLocationForecastSource())

    if shom_tide_enabled():
        sources.append(ShomTideCoefficientSource())

    if os.environ.get("ENABLE_CMEMS", "true").lower() in {"1", "true", "yes"} and cmems_enabled():
        sources.extend([CmemsWavSource(), CmemsPhySource(), CmemsBgcSource()])
    else:
        print("[INFO] CMEMS disabled or credentials missing; using free no-credential sources only.")

    disabled = {
        item.strip()
        for item in os.environ.get("DISABLED_FORECAST_SOURCES", "").split(",")
        if item.strip()
    }
    enabled = {
        item.strip()
        for item in os.environ.get("FORECAST_SOURCES", "").split(",")
        if item.strip()
    }
    if enabled:
        known = {source.config.code for source in sources}
        unknown = sorted(enabled - known)
        if unknown:
            print(f"[WARN] Unknown FORECAST_SOURCES ignored: {', '.join(unknown)}")
        skipped = sorted(known - enabled - disabled)
        if skipped:
            print(f"[INFO] FORECAST_SOURCES allowlist skipped: {', '.join(skipped)}")
        sources = [source for source in sources if source.config.code in enabled]

    return [source for source in sources if source.config.code not in disabled]


def fetch_source_values(
    sources: list[ForecastSource],
    data_repo: Data2LamerForecastRepository,
    r2_archive: R2SourceValueArchive,
    *,
    collect_values: bool = True,
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
                r2_key = r2_archive.write_source_values(
                    source=source.config,
                    run_id=run_id,
                    run_time=run_time,
                    values=values,
                )
                data_repo.upsert_grid_points(values)
                data_repo.finish_run(run_id, "success", rows_count=len(values))
                archive_status = f", R2: {r2_key}" if r2_key else ""
                print(
                    f"[OK] {source.config.code}: {len(values)} source values "
                    f"({inserted} stored in DATA2LAMER{archive_status})"
                )
                if collect_values:
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

    r2_archive = R2SourceValueArchive.from_env()
    if r2_archive.available:
        print(f"[INFO] R2 raw source archive enabled: bucket={r2_archive.bucket} prefix={r2_archive.prefix}")
    else:
        print(
            "[INFO] R2 raw source archive is not configured. Missing settings: "
            + ", ".join(r2_archive.missing_settings())
        )

    push_to_supabase = _env_enabled("FORECAST_PUSH_TO_SUPABASE", "true")
    log_column_counts = _env_enabled("FORECAST_LOG_COLUMN_COUNTS", "true")

    values, run_time = fetch_source_values(
        sources,
        data_repo,
        r2_archive,
        collect_values=push_to_supabase or log_column_counts,
    )
    consolidated_rows: list[dict[str, object]] | None = None
    if log_column_counts:
        log_source_value_metric_counts(values)
        consolidated_rows = consolidate_source_values(values, run_time) if values else []
        log_environment_forecast_column_counts(consolidated_rows)

    if not push_to_supabase:
        print("[OK] Source values archived; VU2LAMER Supabase publication skipped.")
        return

    if not values:
        raise RuntimeError("No forecast source values were fetched.")

    if consolidated_rows is None:
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
