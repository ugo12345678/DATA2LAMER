from __future__ import annotations

from pathlib import Path
import os
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping

from config.settings import (
    BASE_DIR,
    FEATURES_DIR,
    RAW_STATIC_DIR,
    BATHY_DIR,
    COASTLINE_DIR,
    SPOTS_SOURCE_FILE,
    STATIC_FILE,
    STATIC_SAMPLE_FILE,
    STATIC_SUMMARY_FILE,
)

from src.utils.io_utils import (
    ensure_dir,
    assert_file_exists,
    read_csv,
    save_parquet_bundle,
)
from src.utils.logging_utils import (
    print_header,
    log_kv,
    log_file_written,
    log_shape,
    log_df_head,
    log_info,
    log_warning,
)
from src.utils.summary_utils import build_column_summary
from src.utils.geo_utils import (
    read_vector,
    ensure_metric_spots_gdf,
    to_crs_safe,
)

warnings.filterwarnings("ignore", category=UserWarning)

# ============================================================================
# STATIC CONFIG
# ============================================================================
SPOTS_FILE = Path(os.environ.get("SPOTS_FILE_OVERRIDE", str(SPOTS_SOURCE_FILE)))

METRIC_CRS = "EPSG:2154"
BUFFER_RADII_M = [150, 300]

STATIC_REQUIRED_SPOT_COLS = [
    "spot_id",
    "spot_name",
    "spot_lat",
    "spot_lon",
]

STATIC_OPTIONAL_META_COLS = [
    "cluster",
    "coast_orientation_deg",
    "grid_lat",
    "grid_lon",
]


# ============================================================================
# FILE DISCOVERY
# ============================================================================
def find_files_recursive(root: Path, patterns: list[str]) -> list[Path]:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(root.rglob(pattern))
    return sorted({p.resolve() for p in matches if p.is_file()})


def pick_bathymetry_file() -> Path:
    candidates = find_files_recursive(BATHY_DIR, ["*.tif", "*.tiff"])
    if not candidates:
        raise FileNotFoundError(
            f"Aucun raster bathymétrique trouvé dans {BATHY_DIR} "
            f"(recherche récursive *.tif/*.tiff)."
        )

    preferred = sorted(
        candidates,
        key=lambda p: (
            0 if "bathy" in p.name.lower() or "bathym" in p.name.lower() else 1,
            len(str(p)),
        ),
    )
    chosen = preferred[0]
    log_info(f"Bathymétrie sélectionnée: {chosen}")
    return chosen


def coastline_priority(path: Path) -> tuple[int, int, str]:
    name = path.name.lower()

    # On préfère le trait de côte "ligne"
    if "ligne" in name:
        rank = 0
    elif path.suffix.lower() == ".shp":
        rank = 1
    elif path.suffix.lower() == ".gpkg":
        rank = 2
    elif path.suffix.lower() == ".geojson":
        rank = 3
    else:
        rank = 9

    # On évite polygone / fermetures si possible
    if "polygone" in name:
        rank += 3
    if "fermeture" in name or "limar" in name:
        rank += 4

    return (rank, len(str(path)), name)


def pick_coastline_file() -> Path:
    candidates = find_files_recursive(COASTLINE_DIR, ["*.shp", "*.gpkg", "*.geojson"])
    if not candidates:
        raise FileNotFoundError(
            f"Aucun fichier côte trouvé dans {COASTLINE_DIR} "
            f"(recherche récursive *.shp/*.gpkg/*.geojson)."
        )

    chosen = sorted(candidates, key=coastline_priority)[0]
    log_info(f"Trait de côte sélectionné: {chosen}")
    return chosen


# ============================================================================
# DATA LOADING
# ============================================================================
def load_spots(spots_file: Path) -> pd.DataFrame:
    df = read_csv(spots_file, label="SPOTS_FILE")

    missing = [c for c in STATIC_REQUIRED_SPOT_COLS if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes spots manquantes: {missing}")

    cols = STATIC_REQUIRED_SPOT_COLS + [c for c in STATIC_OPTIONAL_META_COLS if c in df.columns]
    df = df[cols].copy()
    df = df.drop_duplicates(subset=["spot_id"]).reset_index(drop=True)

    return df


def load_coastline(coast_file: Path) -> gpd.GeoDataFrame:
    coast = read_vector(coast_file)
    if coast.empty:
        raise ValueError(f"Le fichier côte est vide: {coast_file}")

    coast = to_crs_safe(coast, METRIC_CRS)

    # Dissolve en une seule géométrie pour simplifier la distance
    coast = coast[~coast.geometry.isna()].copy()
    coast = coast[coast.geometry.is_valid].copy()

    if coast.empty:
        raise ValueError(f"Aucune géométrie valide dans le fichier côte: {coast_file}")

    dissolved = gpd.GeoDataFrame(
        {"name": ["coastline"]},
        geometry=[coast.unary_union],
        crs=coast.crs,
    )
    return dissolved


# ============================================================================
# RASTER HELPERS
# ============================================================================
def _read_point_value(src: rasterio.io.DatasetReader, x: float, y: float) -> float:
    try:
        row, col = src.index(x, y)
        if row < 0 or col < 0 or row >= src.height or col >= src.width:
            return np.nan

        arr = src.read(1, masked=True)
        value = arr[row, col]
        if np.ma.is_masked(value):
            return np.nan
        return float(value)
    except Exception:
        return np.nan


def _compute_slope_array(arr: np.ndarray, transform) -> np.ndarray:
    arr = np.asarray(arr, dtype=float)
    arr[arr == 0] = np.nan

    xres = abs(transform.a)
    yres = abs(transform.e)

    if not np.isfinite(xres) or not np.isfinite(yres) or xres == 0 or yres == 0:
        return np.full_like(arr, np.nan, dtype=float)

    gy, gx = np.gradient(arr, yres, xres)
    slope = np.sqrt(gx**2 + gy**2)
    return slope


def _buffer_stats_from_raster(
    src: rasterio.io.DatasetReader,
    geom,
) -> dict[str, float]:
    try:
        out_image, out_transform = mask(
            src,
            [mapping(geom)],
            crop=True,
            filled=False,
        )
    except Exception:
        return {}

    arr = out_image[0]
    if arr.size == 0:
        return {}

    data = np.array(arr, dtype=float)
    if np.ma.isMaskedArray(arr):
        mask_arr = ~arr.mask
        data = np.where(mask_arr, data, np.nan)

    # convention empirique: on traite 0 comme "terre / pas d'eau" pour la bathy
    data[data == 0] = np.nan

    valid = data[np.isfinite(data)]
    if valid.size == 0:
        return {}

    slope = _compute_slope_array(data, out_transform)
    slope_valid = slope[np.isfinite(slope)]

    stats = {
        "bathy_mean": float(np.nanmean(valid)),
        "bathy_std": float(np.nanstd(valid)),
        "bathy_min": float(np.nanmin(valid)),
        "bathy_max": float(np.nanmax(valid)),
        "slope_mean": float(np.nanmean(slope_valid)) if slope_valid.size else np.nan,
        "slope_std": float(np.nanstd(slope_valid)) if slope_valid.size else np.nan,
        "slope_max": float(np.nanmax(slope_valid)) if slope_valid.size else np.nan,
    }
    return stats


# ============================================================================
# FEATURE ENGINEERING
# ============================================================================
def compute_static_features_for_spot(
    src: rasterio.io.DatasetReader,
    coast_union: gpd.GeoDataFrame,
    spot_row,
) -> dict:
    geom = spot_row.geometry
    x = geom.x
    y = geom.y

    row = {col: spot_row[col] for col in spot_row.index if col != "geometry"}

    row["static_data_ok"] = 1
    row["bathy_point"] = _read_point_value(src, x, y)

    coast_geom = coast_union.geometry.iloc[0]
    row["dist_coast_m"] = float(geom.distance(coast_geom))

    for radius in BUFFER_RADII_M:
        stats = _buffer_stats_from_raster(src, geom.buffer(radius))
        for base_name in [
            "bathy_mean",
            "bathy_std",
            "bathy_min",
            "bathy_max",
            "slope_mean",
            "slope_std",
            "slope_max",
        ]:
            row[f"{base_name}_{radius}m"] = stats.get(base_name, np.nan)

    return row


def build_static_features(spots_df: pd.DataFrame, bathy_file: Path, coast_file: Path) -> pd.DataFrame:
    spots_gdf = ensure_metric_spots_gdf(
        spots_df,
        lon_col="spot_lon",
        lat_col="spot_lat",
        metric_crs=METRIC_CRS,
    )

    coast_union = load_coastline(coast_file)

    rows: list[dict] = []
    with rasterio.open(bathy_file) as src:
        # reprojette les spots vers le CRS du raster pour lecture/masque
        spots_raster_crs = spots_gdf.to_crs(src.crs)
        coast_raster_crs = to_crs_safe(coast_union, src.crs).to_crs(METRIC_CRS)
        coast_metric = to_crs_safe(coast_union, METRIC_CRS)

        # distance côte en métrique, stats raster dans le CRS raster
        for idx in range(len(spots_gdf)):
            spot_metric = spots_gdf.iloc[idx]
            spot_raster = spots_raster_crs.iloc[idx]

            merged_row = spot_metric.copy()
            merged_row.geometry = spot_raster.geometry

            static_row = compute_static_features_for_spot(
                src=src,
                coast_union=coast_metric,
                spot_row=merged_row,
            )
            rows.append(static_row)

    out = pd.DataFrame(rows)

    # ordre des colonnes
    preferred_cols = (
        STATIC_REQUIRED_SPOT_COLS
        + [c for c in STATIC_OPTIONAL_META_COLS if c in out.columns]
        + ["static_data_ok", "bathy_point"]
        + [f"bathy_mean_{r}m" for r in BUFFER_RADII_M]
        + [f"bathy_std_{r}m" for r in BUFFER_RADII_M]
        + [f"bathy_min_{r}m" for r in BUFFER_RADII_M]
        + [f"bathy_max_{r}m" for r in BUFFER_RADII_M]
        + [f"slope_mean_{r}m" for r in BUFFER_RADII_M]
        + [f"slope_std_{r}m" for r in BUFFER_RADII_M]
        + [f"slope_max_{r}m" for r in BUFFER_RADII_M]
        + ["dist_coast_m"]
    )

    remaining_cols = [c for c in out.columns if c not in preferred_cols]
    out = out[[c for c in preferred_cols if c in out.columns] + remaining_cols].copy()

    return out.sort_values("spot_id").reset_index(drop=True)


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    print_header("RUN SCRIPT: 05b_build_static_pipeline.py")

    ensure_dir(FEATURES_DIR)
    ensure_dir(RAW_STATIC_DIR)
    ensure_dir(BATHY_DIR)
    ensure_dir(COASTLINE_DIR)

    log_kv("BASE_DIR", BASE_DIR)
    log_kv("RAW_DIR", RAW_STATIC_DIR)
    log_kv("PROCESSED_DIR", FEATURES_DIR)
    log_kv("SPOTS_FILE", SPOTS_FILE)

    assert_file_exists(SPOTS_FILE, label="SPOTS_FILE")
    spots_df = load_spots(SPOTS_FILE)
    log_info(f"{len(spots_df)} spot(s) chargé(s)")

    bathy_file = pick_bathymetry_file()
    coast_file = pick_coastline_file()

    log_kv("BATHY", bathy_file)
    log_kv("COAST", coast_file)

    static_df = build_static_features(
        spots_df=spots_df,
        bathy_file=bathy_file,
        coast_file=coast_file,
    )

    log_shape("STATIC", static_df)
    log_df_head(static_df, n=5)

    summary_df = build_column_summary(static_df)

    written = save_parquet_bundle(
        df=static_df,
        parquet_path=STATIC_FILE,
        sample_path=STATIC_SAMPLE_FILE,
        summary_df=summary_df,
        summary_path=STATIC_SUMMARY_FILE,
        index=False,
    )

    for path in written.values():
        log_file_written(path)


if __name__ == "__main__":
    main()