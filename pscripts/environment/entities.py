from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class SourceConfig:
    code: str
    name: str
    provider: str
    kind: str
    enabled: bool = True


@dataclass
class SourceValue:
    spot_id: str
    source_code: str
    valid_time: datetime
    metric: str
    value: float | None
    unit: str
    fetched_at: datetime
    raw_variable: str
    run_id: str | None = None
    model: str | None = None
    resolution_minutes: int | None = None
    grid_lat: float | None = None
    grid_lon: float | None = None
    quality_flags: dict[str, Any] = field(default_factory=dict)

    def to_data2lamer_row(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source_code": self.source_code,
            "spot_id": self.spot_id,
            "valid_time": self.valid_time.isoformat(),
            "metric": self.metric,
            "value": self.value,
            "unit": self.unit,
            "raw_variable": self.raw_variable,
            "model": self.model,
            "resolution_minutes": self.resolution_minutes,
            "grid_lat": self.grid_lat,
            "grid_lon": self.grid_lon,
            "quality_flags": self.quality_flags,
            "fetched_at": self.fetched_at.isoformat(),
        }

