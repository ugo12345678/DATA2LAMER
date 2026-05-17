from __future__ import annotations

import html
import os
import re
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.units import normalize_metric_value


REQUEST_TIMEOUT = 60

MONTH_NUMBERS = {
    "janvier": 1,
    "fevrier": 2,
    "février": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "août": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
    "décembre": 12,
}


class _TableRowTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self.texts: list[str] = []
        self._current_row: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "tr":
            self._current_row = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "tr" and self._current_row is not None:
            row = [item for item in self._current_row if item]
            if row:
                self.rows.append(row)
            self._current_row = None

    def handle_data(self, data: str) -> None:
        for segment in data.splitlines():
            text = _normalize_text(segment)
            if text:
                self.texts.append(text)
            if self._current_row is not None and text:
                self._current_row.append(text)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value).replace("\xa0", " ")).strip()


def _month_year_from_text(value: str) -> tuple[int, int] | None:
    match = re.search(
        r"\b("
        + "|".join(re.escape(month) for month in MONTH_NUMBERS)
        + r")\s+(\d{4})\b",
        value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return MONTH_NUMBERS[match.group(1).lower()], int(match.group(2))


def _day_from_cell(value: str) -> int | None:
    match = re.match(r"^(\d{1,2})(?:\s+[A-ZÀ-Ÿ])?$", value, flags=re.IGNORECASE)
    if not match:
        return None
    day = int(match.group(1))
    if 1 <= day <= 31:
        return day
    return None


def _coefficient_from_cell(value: str) -> int | None:
    if not re.fullmatch(r"\d{2,3}", value):
        return None
    coefficient = int(value)
    if 20 <= coefficient <= 120:
        return coefficient
    return None


def _coefficients_from_text(value: str) -> list[int]:
    coefficients = []
    for match in re.findall(r"\b\d{2,3}\b", value):
        coefficient = _coefficient_from_cell(match)
        if coefficient is not None:
            coefficients.append(coefficient)
    return coefficients


def _is_calendar_end_text(value: str) -> bool:
    text = value.lower()
    return (
        text.startswith("afficher les dates")
        or "grande marée" in text
        or "grande maree" in text
        or "marée de vive-eau" in text
        or "maree de vive-eau" in text
    )


def _parse_coefficients_from_rows(rows: list[list[str]]) -> dict[date, list[int]]:
    current_month: int | None = None
    current_year: int | None = None
    coefficients_by_date: dict[date, list[int]] = {}

    for row in rows:
        row_text = " ".join(row)
        month_year = _month_year_from_text(row_text)
        if month_year is not None:
            current_month, current_year = month_year
            continue

        if current_month is None or current_year is None:
            continue

        day_index = None
        day = None
        for index, cell in enumerate(row):
            parsed_day = _day_from_cell(cell)
            if parsed_day is not None:
                day_index = index
                day = parsed_day
                break

        if day_index is None or day is None:
            continue

        coefficients: list[int] = []
        for cell in row[day_index + 1 :]:
            coefficients.extend(_coefficients_from_text(cell))
        if coefficients:
            coefficients_by_date[date(current_year, current_month, day)] = coefficients

    return coefficients_by_date


def _parse_coefficients_from_texts(texts: list[str]) -> dict[date, list[int]]:
    current_month: int | None = None
    current_year: int | None = None
    current_date: date | None = None
    coefficients_by_date: dict[date, list[int]] = {}

    for text in texts:
        if _is_calendar_end_text(text):
            break

        month_year = _month_year_from_text(text)
        if month_year is not None:
            current_month, current_year = month_year
            current_date = None
            continue

        if current_month is None or current_year is None:
            continue

        day = _day_from_cell(text)
        if day is not None:
            current_date = date(current_year, current_month, day)
            coefficients_by_date.setdefault(current_date, [])
            continue

        if current_date is None:
            continue

        coefficients_by_date[current_date].extend(_coefficients_from_text(text))

    return {key: values for key, values in coefficients_by_date.items() if values}


def parse_maree_info_coefficients(document: str) -> dict[date, list[int]]:
    parser = _TableRowTextParser()
    parser.feed(document)
    coefficients_by_date = _parse_coefficients_from_rows(parser.rows)
    if coefficients_by_date:
        return coefficients_by_date

    return _parse_coefficients_from_texts(parser.texts)


def _hourly_times(start: datetime, end: datetime):
    current = start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    while current < end:
        yield current
        current += timedelta(hours=1)


class MareeInfoTideCoefficientSource(ForecastSource):
    config = SourceConfig(
        code="maree_info_tide_coefficients",
        name="maree.info Tide Coefficients",
        provider="maree.info",
        kind="tide",
    )

    def __init__(self) -> None:
        self.session = requests.Session()
        self.forecast_days = int(os.environ.get("FORECAST_DAYS", "7"))
        self.port_id = os.environ.get("MAREE_INFO_PORT_ID", "82")
        self.base_url = os.environ.get("MAREE_INFO_BASE_URL", "https://maree.info")
        self.target_tz = ZoneInfo(os.environ.get("FORECAST_TARGET_TIMEZONE", "Europe/Paris"))
        self.daily_reducer = os.environ.get("TIDE_COEFFICIENT_DAILY_REDUCER", "max").lower()

    def fetch(self, spots: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        coefficients_by_date = self._fetch_coefficients()
        window_end = run_time + timedelta(days=self.forecast_days)

        rows: list[SourceValue] = []
        for valid_time in _hourly_times(run_time, window_end):
            local_date = valid_time.astimezone(self.target_tz).date()
            coefficients = coefficients_by_date.get(local_date)
            if not coefficients:
                continue

            coefficient_value = self._daily_value(coefficients)
            normalized, unit = normalize_metric_value("tide_coefficient", coefficient_value, "coef")
            if normalized is None:
                continue

            for _, spot in spots.iterrows():
                rows.append(
                    SourceValue(
                        spot_id=str(spot["spot_id"]),
                        source_code=self.config.code,
                        valid_time=valid_time,
                        metric="tide_coefficient",
                        value=normalized,
                        unit=unit,
                        raw_variable=f"daily_tide_coefficient_{self.daily_reducer}",
                        fetched_at=run_time,
                        model=f"port_id={self.port_id}",
                        resolution_minutes=1440,
                        quality_flags={
                            "daily_reducer": self.daily_reducer,
                            "reference_port_id": self.port_id,
                            "source_url": self._url(),
                        },
                    )
                )

        return rows

    def _fetch_coefficients(self) -> dict[date, list[int]]:
        response = self.session.get(self._url(), timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return parse_maree_info_coefficients(response.text)

    def _daily_value(self, coefficients: list[int]) -> float:
        if self.daily_reducer == "mean":
            return sum(coefficients) / len(coefficients)
        if self.daily_reducer == "min":
            return float(min(coefficients))
        return float(max(coefficients))

    def _url(self) -> str:
        return f"{self.base_url.rstrip('/')}/{self.port_id}/coefficients"


def maree_info_enabled() -> bool:
    return os.environ.get("ENABLE_MAREE_INFO_TIDES", "true").lower() in {"1", "true", "yes"}
