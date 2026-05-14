from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from pscripts.environment.entities import SourceConfig, SourceValue


class ForecastSource(ABC):
    config: SourceConfig

    @abstractmethod
    def fetch(self, spots: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        """Fetch and normalize source values for the given spots."""

