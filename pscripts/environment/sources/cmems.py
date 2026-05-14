from __future__ import annotations

import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.timeutils import floor_hour, parse_utc
from pscripts.environment.units import normalize_metric_value, to_float


SPOT_MARGIN_DEG = float(os.environ.get("SPOT_MARGIN_DEG", "0.03"))
CMEMS_MAX_WORKERS = int(os.environ.get("CMEMS_MAX_WORKERS", "6"))


def cmems_enabled() -> bool:
    return bool(os.getenv("CMEMS_USERNAME") and os.getenv("CMEMS_PASSWORD"))


def _forecast_window(run_time: datetime) -> tuple[str, str]:
    forecast_days = int(os.environ.get("FORECAST_DAYS", "7"))
    start = run_time
    end = start + timedelta(days=forecast_days)
    return start.isoformat(), end.isoformat()


def _spot_bbox(spot: pd.Series) -> dict[str, float]:
    return {
        "minimum_longitude": float(spot["longitude_min"]) - SPOT_MARGIN_DEG,
        "maximum_longitude": float(spot["longitude_max"]) + SPOT_MARGIN_DEG,
        "minimum_latitude": float(spot["latitude_min"]) - SPOT_MARGIN_DEG,
        "maximum_latitude": float(spot["latitude_max"]) + SPOT_MARGIN_DEG,
    }


def _standardize_coords(ds):
    rename_map = {}
    if "latitude" in ds.coords and "lat" not in ds.coords:
        rename_map["latitude"] = "lat"
    if "longitude" in ds.coords and "lon" not in ds.coords:
        rename_map["longitude"] = "lon"
    return ds.rename(rename_map) if rename_map else ds


def _maybe_select_surface(ds):
    for depth_name in ["depth", "depthu", "depthv", "deptht", "lev"]:
        if depth_name in ds.dims or depth_name in ds.coords:
            try:
                ds = ds.isel({depth_name: 0})
            except Exception:
                pass
    return ds


def _open_dataset(dataset_id: str, spot: pd.Series, run_time: datetime, select_surface: bool):
    import copernicusmarine

    start_dt, end_dt = _forecast_window(run_time)
    ds = copernicusmarine.open_dataset(
        dataset_id=dataset_id,
        username=os.environ["CMEMS_USERNAME"],
        password=os.environ["CMEMS_PASSWORD"],
        start_datetime=start_dt,
        end_datetime=end_dt,
        coordinates_selection_method="nearest",
        **_spot_bbox(spot),
    )
    ds = _standardize_coords(ds)
    if select_surface:
        ds = _maybe_select_surface(ds)
    return ds


def _pick_available_vars(ds, var_map: dict[str, list[str]]) -> dict[str, str]:
    picked: dict[str, str] = {}
    for metric, aliases in var_map.items():
        for alias in aliases:
            if alias in ds.data_vars:
                picked[metric] = alias
                break
    return picked


def _scalar_coord(point, name: str) -> float | None:
    if name not in point.coords:
        return None
    try:
        value = point[name].values
        if hasattr(value, "item"):
            value = value.item()
        return float(value)
    except Exception:
        return None


def _current_direction_deg(u_value: float, v_value: float) -> float:
    # u is eastward, v is northward. This returns the direction the current flows toward.
    return (math.degrees(math.atan2(u_value, v_value)) + 360.0) % 360.0


def _resolution_minutes(df: pd.DataFrame) -> int | None:
    times = pd.to_datetime(df["time"], errors="coerce", utc=True).dropna().sort_values().drop_duplicates()
    if len(times) < 2:
        return None
    delta = times.diff().dropna().median()
    return int(delta.total_seconds() // 60)


class CmemsSource(ForecastSource):
    dataset_id: str
    variable_map: dict[str, list[str]]
    select_surface = False
    default_units: dict[str, str] = {}

    def fetch(self, spots: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        if not cmems_enabled():
            print(f"[INFO] {self.config.code} skipped: CMEMS credentials are not configured.")
            return []

        rows: list[SourceValue] = []
        with ThreadPoolExecutor(max_workers=CMEMS_MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._fetch_spot, spot, run_time): spot["spot_id"]
                for _, spot in spots.iterrows()
            }
            for future in as_completed(futures):
                spot_id = futures[future]
                try:
                    rows.extend(future.result())
                except Exception as exc:
                    print(f"[WARN] {self.config.code} failed for spot {spot_id}: {exc}")
        return rows

    def _fetch_spot(self, spot: pd.Series, run_time: datetime) -> list[SourceValue]:
        ds = _open_dataset(self.dataset_id, spot, run_time, self.select_surface)
        picked = _pick_available_vars(ds, self.variable_map)
        if not picked:
            return []

        ds = ds[list(picked.values())].rename({v: k for k, v in picked.items()})
        point = ds.sel(lat=float(spot["lat_center"]), lon=float(spot["lon_center"]), method="nearest")
        df = point[list(picked.keys())].to_dataframe().reset_index()
        if df.empty or "time" not in df.columns:
            return []

        grid_lat = _scalar_coord(point, "lat")
        grid_lon = _scalar_coord(point, "lon")
        resolution = _resolution_minutes(df)
        rows: list[SourceValue] = []

        for _, frame_row in df.iterrows():
            valid_time = floor_hour(parse_utc(frame_row["time"]))
            metric_values = self._metric_values(frame_row)

            for metric, raw_value in metric_values.items():
                unit_guess = self.default_units.get(metric)
                normalized, unit = normalize_metric_value(metric, raw_value, unit_guess)
                if normalized is None:
                    continue

                rows.append(
                    SourceValue(
                        spot_id=str(spot["spot_id"]),
                        source_code=self.config.code,
                        valid_time=valid_time,
                        metric=metric,
                        value=normalized,
                        unit=unit,
                        raw_variable=metric,
                        fetched_at=run_time,
                        model=self.dataset_id,
                        resolution_minutes=resolution,
                        grid_lat=grid_lat,
                        grid_lon=grid_lon,
                    )
                )

        return rows

    def _metric_values(self, frame_row: pd.Series) -> dict[str, Any]:
        return {metric: frame_row[metric] for metric in self.variable_map if metric in frame_row}


class CmemsWavSource(CmemsSource):
    config = SourceConfig(
        code="cmems_ibi_wav",
        name="Copernicus Marine IBI Waves",
        provider="copernicus-marine",
        kind="marine",
    )
    dataset_id = os.environ.get("CMEMS_WAV_DATASET_ID", "cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i")
    variable_map = {
        "wave_height": ["VHM0", "swh", "hm0"],
        "wave_period": ["VTM10", "mwp", "tm10"],
        "wave_direction": ["VMDR", "mwd"],
    }
    default_units = {
        "wave_height": "m",
        "wave_period": "s",
        "wave_direction": "deg",
    }


class CmemsPhySource(CmemsSource):
    config = SourceConfig(
        code="cmems_ibi_phy",
        name="Copernicus Marine IBI Physical",
        provider="copernicus-marine",
        kind="marine",
    )
    dataset_id = os.environ.get("CMEMS_PHY_DATASET_ID", "cmems_mod_ibi_phy_anfc_0.027deg-3D_P1D-m")
    select_surface = True
    variable_map = {
        "water_temperature": ["thetao", "bottomT", "tos"],
        "salinity": ["so", "sos"],
        "current_u": ["uo", "vozocrtx"],
        "current_v": ["vo", "vomecrty"],
    }
    default_units = {
        "water_temperature": "degC",
        "salinity": "psu",
        "current_speed": "m/s",
        "current_direction": "deg",
    }

    def _metric_values(self, frame_row: pd.Series) -> dict[str, Any]:
        values: dict[str, Any] = {}
        if "water_temperature" in frame_row:
            values["water_temperature"] = frame_row["water_temperature"]
        if "salinity" in frame_row:
            values["salinity"] = frame_row["salinity"]

        u_value = to_float(frame_row.get("current_u"))
        v_value = to_float(frame_row.get("current_v"))
        if u_value is not None and v_value is not None:
            values["current_speed"] = (u_value**2 + v_value**2) ** 0.5
            values["current_direction"] = _current_direction_deg(u_value, v_value)

        return values


class CmemsBgcSource(CmemsSource):
    config = SourceConfig(
        code="cmems_ibi_bgc",
        name="Copernicus Marine IBI Biogeochemistry",
        provider="copernicus-marine",
        kind="marine",
    )
    dataset_id = os.environ.get("CMEMS_BGC_DATASET_ID", "cmems_mod_ibi_bgc_anfc_0.027deg-3D_P1D-m")
    select_surface = True
    variable_map = {
        "chlorophyll": ["chl", "CHL", "chl1", "chlorophyll"],
        "light_attenuation": ["kd", "kd490", "KD490", "att", "light_attenuation", "mldr10_1"],
    }
    default_units = {
        "chlorophyll": "mg/m3",
        "light_attenuation": "1/m",
    }

