from __future__ import annotations

from pathlib import Path
import geopandas as gpd
import pandas as pd


WGS84 = "EPSG:4326"


def read_vector(path: Path | str) -> gpd.GeoDataFrame:
    return gpd.read_file(path)


def ensure_crs(gdf: gpd.GeoDataFrame, crs: str = WGS84) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(crs)
    return gdf


def to_crs_safe(gdf: gpd.GeoDataFrame, crs: str) -> gpd.GeoDataFrame:
    gdf = ensure_crs(gdf)
    if str(gdf.crs) == str(crs):
        return gdf
    return gdf.to_crs(crs)


def spots_to_gdf(
    df: pd.DataFrame,
    lon_col: str = "spot_lon",
    lat_col: str = "spot_lat",
    crs: str = WGS84,
) -> gpd.GeoDataFrame:
    if lon_col not in df.columns or lat_col not in df.columns:
        raise KeyError(f"Colonnes manquantes pour créer la géométrie: {lon_col}, {lat_col}")

    gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs=crs,
    )
    return gdf


def estimate_metric_crs_for_france() -> str:
    return "EPSG:2154"


def ensure_metric_spots_gdf(
    df: pd.DataFrame,
    lon_col: str = "spot_lon",
    lat_col: str = "spot_lat",
    metric_crs: str = "EPSG:2154",
) -> gpd.GeoDataFrame:
    gdf = spots_to_gdf(df, lon_col=lon_col, lat_col=lat_col, crs=WGS84)
    return gdf.to_crs(metric_crs)