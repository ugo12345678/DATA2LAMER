from __future__ import annotations

import math
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.sources.base import ForecastSource
from pscripts.environment.timeutils import floor_hour, parse_utc
from pscripts.environment.units import normalize_metric_value, to_float


SPOT_MARGIN_DEG = float(os.environ.get("SPOT_MARGIN_DEG", "0.03"))


def cmems_enabled() -> bool:
    return bool(os.getenv("CMEMS_USERNAME") and os.getenv("CMEMS_PASSWORD"))


def _forecast_window(run_time: datetime) -> tuple[str, str]:
    forecast_days = int(os.environ.get("FORECAST_DAYS", "7"))
    start = run_time
    end = start + timedelta(days=forecast_days)
    return start.isoformat(), end.isoformat()


def _spots_bbox(spots: pd.DataFrame) -> dict[str, float]:
    return {
        "minimum_longitude": float(spots["longitude_min"].min()) - SPOT_MARGIN_DEG,
        "maximum_longitude": float(spots["longitude_max"].max()) + SPOT_MARGIN_DEG,
        "minimum_latitude": float(spots["latitude_min"].min()) - SPOT_MARGIN_DEG,
        "maximum_latitude": float(spots["latitude_max"].max()) + SPOT_MARGIN_DEG,
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


def _open_dataset(
    dataset_id: str,
    spots: pd.DataFrame,
    run_time: datetime,
    select_surface: bool,
    variables: list[str] | None = None,
):
    import copernicusmarine

    start_dt, end_dt = _forecast_window(run_time)
    bbox = _spots_bbox(spots)
    request_kwargs: dict[str, Any] = {
        "dataset_id": dataset_id,
        "username": os.environ["CMEMS_USERNAME"],
        "password": os.environ["CMEMS_PASSWORD"],
        "start_datetime": start_dt,
        "end_datetime": end_dt,
        "coordinates_selection_method": os.environ.get("CMEMS_COORDINATES_SELECTION_METHOD", "nearest"),
        **bbox,
    }
    if variables:
        request_kwargs["variables"] = variables
    if select_surface and os.environ.get("CMEMS_SUBSET_SURFACE_DEPTH", "true").lower() in {"1", "true", "yes"}:
        request_kwargs["minimum_depth"] = float(os.environ.get("CMEMS_SURFACE_DEPTH_M", "0"))
        request_kwargs["maximum_depth"] = float(os.environ.get("CMEMS_SURFACE_DEPTH_M", "0"))

    print(
        "[INFO] CMEMS opening dataset "
        f"{dataset_id} bbox=({bbox['minimum_latitude']:.3f},{bbox['minimum_longitude']:.3f})"
        f"-({bbox['maximum_latitude']:.3f},{bbox['maximum_longitude']:.3f})"
        f" variables={','.join(variables or ['all'])}"
    )
    ds = copernicusmarine.open_dataset(**request_kwargs)
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


def _dataset_resolution_minutes(ds) -> int | None:
    if "time" not in ds.coords:
        return None
    try:
        times = pd.to_datetime(ds.coords["time"].values, errors="coerce", utc=True)
    except Exception:
        return None
    times = pd.Series(times).dropna().sort_values().drop_duplicates()
    if len(times) < 2:
        return None
    delta = times.diff().dropna().median()
    return int(delta.total_seconds() // 60)


class CmemsSource(ForecastSource):
    dataset_id: str
    variable_map: dict[str, list[str]]
    requested_variables: list[str] | None = None
    select_surface = False
    default_units: dict[str, str] = {}

    def fetch(self, spots: pd.DataFrame, run_time: datetime) -> list[SourceValue]:
        if not cmems_enabled():
            print(f"[INFO] {self.config.code} skipped: CMEMS credentials are not configured.")
            return []

        requested_variables = self._requested_variables()
        try:
            ds = _open_dataset(self.dataset_id, spots, run_time, self.select_surface, requested_variables)
        except Exception:
            allow_unfiltered_fallback = (
                os.environ.get("CMEMS_ALLOW_UNFILTERED_FALLBACK", "false").lower() in {"1", "true", "yes"}
            )
            if not requested_variables or not allow_unfiltered_fallback:
                raise
            print(
                f"[WARN] {self.config.code}: variable-filtered CMEMS open failed; "
                "retrying without variable filter."
            )
            ds = _open_dataset(self.dataset_id, spots, run_time, self.select_surface)

        picked = _pick_available_vars(ds, self.variable_map)
        if not picked:
            print(f"[WARN] {self.config.code} skipped: no expected variables found in {self.dataset_id}.")
            return []

        ds = ds[list(picked.values())].rename({v: k for k, v in picked.items()})
        if os.environ.get("CMEMS_LOAD_SUBSET_IN_MEMORY", "true").lower() in {"1", "true", "yes"}:
            ds = ds.load()
        resolution = _dataset_resolution_minutes(ds)
        rows: list[SourceValue] = []
        for _, spot in spots.iterrows():
            try:
                rows.extend(self._values_for_spot(ds, spot, run_time, resolution))
            except Exception as exc:
                print(f"[WARN] {self.config.code} failed for spot {spot['spot_id']}: {exc}")
        return rows

    def _values_for_spot(
        self,
        ds,
        spot: pd.Series,
        run_time: datetime,
        resolution: int | None,
    ) -> list[SourceValue]:
        point = ds.sel(lat=float(spot["lat_center"]), lon=float(spot["lon_center"]), method="nearest")
        selected_metrics = [metric for metric in self.variable_map if metric in point]
        if not selected_metrics:
            return []

        df = point[selected_metrics].to_dataframe().reset_index()
        if df.empty or "time" not in df.columns:
            return []

        grid_lat = _scalar_coord(point, "lat")
        grid_lon = _scalar_coord(point, "lon")
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

    def _requested_variables(self) -> list[str] | None:
        configured = os.environ.get(f"{self.config.code.upper()}_VARIABLES")
        if configured:
            return [item.strip() for item in configured.split(",") if item.strip()]
        return self.requested_variables


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
    requested_variables = ["VHM0", "VTM10", "VMDR"]
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
    requested_variables = ["thetao", "so", "uo", "vo"]
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
        "phytoplankton_carbon": ["phyc", "PHY", "phytoplankton_carbon"],
        "net_primary_production": ["nppv", "NPPV", "net_primary_production"],
        "euphotic_depth": ["zeu", "ZEU", "euphotic_depth"],
        "light_attenuation": ["kd", "kd490", "KD490", "att", "light_attenuation", "mldr10_1"],
    }
    requested_variables = ["chl", "phyc", "nppv", "zeu"]
    default_units = {
        "chlorophyll": "mg/m3",
        "phytoplankton_carbon": "mmol/m3",
        "net_primary_production": "mg/m3/day",
        "euphotic_depth": "m",
        "light_attenuation": "1/m",
    }

    def _metric_values(self, frame_row: pd.Series) -> dict[str, Any]:
        values = super()._metric_values(frame_row)
        chlorophyll = to_float(values.get("chlorophyll"))
        bloom_risk = _algal_bloom_risk(chlorophyll)
        if bloom_risk is not None:
            values["algal_bloom_risk"] = bloom_risk
        return values


def _algal_bloom_risk(chlorophyll_mg_m3: float | None) -> float | None:
    if chlorophyll_mg_m3 is None:
        return None
    low = float(os.environ.get("ALGAL_BLOOM_CHL_LOW_MG_M3", "3"))
    high = float(os.environ.get("ALGAL_BLOOM_CHL_HIGH_MG_M3", "10"))
    if high <= low:
        return None
    if chlorophyll_mg_m3 <= low:
        return 0.0
    if chlorophyll_mg_m3 >= high:
        return 1.0
    return (chlorophyll_mg_m3 - low) / (high - low)
