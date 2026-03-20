from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import copernicusmarine
import pandas as pd
import xarray as xr


CMEMS_USERNAME = os.environ["CMEMS_USERNAME"]
CMEMS_PASSWORD = os.environ["CMEMS_PASSWORD"]
FORECAST_DAYS = int(os.environ.get("FORECAST_DAYS", "7"))
ZONE_MARGIN_DEG = float(os.environ.get("ZONE_MARGIN_DEG", "0.03"))


def forecast_window() -> tuple[str, str]:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end = now + timedelta(days=FORECAST_DAYS)
    return now.isoformat(), end.isoformat()


def zone_bbox(zone: pd.Series, margin_deg: float = ZONE_MARGIN_DEG) -> dict:
    return {
        "minimum_longitude": float(zone["longitude_min"]) - margin_deg,
        "maximum_longitude": float(zone["longitude_max"]) + margin_deg,
        "minimum_latitude": float(zone["latitude_min"]) - margin_deg,
        "maximum_latitude": float(zone["latitude_max"]) + margin_deg,
    }


def open_cmems_dataset(
    dataset_id: str,
    variables: list[str] | None,
    zone: pd.Series,
    select_surface: bool = False,
) -> xr.Dataset:
    start_dt, end_dt = forecast_window()
    bbox = zone_bbox(zone)

    ds = copernicusmarine.open_dataset(
        dataset_id=dataset_id,
        username=CMEMS_USERNAME,
        password=CMEMS_PASSWORD,
        variables=variables,
        start_datetime=start_dt,
        end_datetime=end_dt,
        coordinates_selection_method="nearest",
        **bbox,
    )

    ds = standardize_coords(ds)

    if select_surface:
        ds = maybe_select_surface(ds)

    return ds


def standardize_coords(ds: xr.Dataset) -> xr.Dataset:
    rename_map = {}
    if "latitude" in ds.coords and "lat" not in ds.coords:
        rename_map["latitude"] = "lat"
    if "longitude" in ds.coords and "lon" not in ds.coords:
        rename_map["longitude"] = "lon"
    return ds.rename(rename_map) if rename_map else ds


def maybe_select_surface(ds: xr.Dataset) -> xr.Dataset:
    for depth_name in ["depth", "depthu", "depthv", "deptht", "lev"]:
        if depth_name in ds.dims or depth_name in ds.coords:
            try:
                ds = ds.isel({depth_name: 0})
            except Exception:
                pass
    return ds


def add_zone_metadata(df: pd.DataFrame, zone: pd.Series, point: xr.Dataset | None = None) -> pd.DataFrame:
    out = df.copy()
    out["zone_id"] = zone["zone_id"]
    out["zone_name"] = zone["zone_name"]
    out["latitude_min"] = float(zone["latitude_min"])
    out["latitude_max"] = float(zone["latitude_max"])
    out["longitude_min"] = float(zone["longitude_min"])
    out["longitude_max"] = float(zone["longitude_max"])
    out["lat_center"] = float(zone["lat_center"])
    out["lon_center"] = float(zone["lon_center"])

    if point is not None:
        if "lat" in point.coords:
            out["grid_lat"] = float(point["lat"].values)
        if "lon" in point.coords:
            out["grid_lon"] = float(point["lon"].values)

    out["time"] = pd.to_datetime(out["time"], errors="coerce")
    out["date"] = out["time"].dt.floor("D")
    out["year"] = out["date"].dt.year
    out["month"] = out["date"].dt.month
    out["dayofyear"] = out["date"].dt.dayofyear
    return out


def base_group_cols(df: pd.DataFrame) -> list[str]:
    cols = [
        "date",
        "zone_id",
        "zone_name",
        "latitude_min",
        "latitude_max",
        "longitude_min",
        "longitude_max",
        "lat_center",
        "lon_center",
        "year",
        "month",
        "dayofyear",
    ]
    for optional in ["grid_lat", "grid_lon"]:
        if optional in df.columns:
            cols.append(optional)
    return cols