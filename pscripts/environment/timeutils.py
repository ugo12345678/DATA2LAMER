from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def utc_now_hour() -> datetime:
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def parse_utc(value) -> datetime:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        raise ValueError(f"Invalid timestamp: {value!r}")
    return ts.to_pydatetime()


def floor_hour(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def horizon_hours(valid_time: datetime, run_time: datetime) -> int:
    delta = valid_time - run_time
    return int(delta.total_seconds() // 3600)

