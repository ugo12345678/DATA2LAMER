from __future__ import annotations

import os
import time
from pathlib import Path

import copernicusmarine
import pandas as pd
import xarray as xr

from config.settings import FEATURES_DIR, RAW_DIR, SPOTS_SOURCE_FILE
from src.utils.io_utils import ensure_dir, read_csv, save_parquet_bundle
from src.utils.logging_utils import log_df_head, log_file_written, log_info, log_kv
from src.utils.summary_utils import build_column_summary

RAW_WAV_DIR = RAW_DIR / "wav"
SPOTS_FILE = Path(os.environ.get("SPOTS_FILE_OVERRIDE", str(SPOTS_SOURCE_FILE)))

DATASET_ID = "cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i"
YEARS = [2024, 2025, 2026]
YEAR_FILTER = "*"
YEARS_LABEL = f"{min(YEARS)}_{max(YEARS)}"
DOWNLOAD_IF_MISSING = False
VAR_CANDIDATES = {
    "wave_height": ["VHM0", "swh", "hm0"],
    "wave_period": ["VTM10", "mwp", "tm10"],
    "wave_direction": ["VMDR", "mwd"],
}
MARGIN_DEG = 0.20
OUT_FILE = FEATURES_DIR / f"features_wav_daily_by_spot_{YEARS_LABEL}.parquet"
OUT_SAMPLE_FILE = FEATURES_DIR / "features_wav_daily_by_spot_sample.parquet"
OUT_SUMMARY_FILE = FEATURES_DIR / "features_wav_daily_by_spot_summary.csv"
REQUIRED_SPOT_COLS = ["spot_id", "name", "lat", "lon"]
OPTIONAL_SPOT_COLS = ["coast_orientation_deg", "cluster"]


def load_spots(spots_file: Path) -> pd.DataFrame:
    spots = read_csv(spots_file, label="SPOTS_FILE")
    missing = [c for c in REQUIRED_SPOT_COLS if c not in spots.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans {spots_file}: {missing}")
    return spots.copy()


def compute_bbox(spots: pd.DataFrame, margin_deg: float = MARGIN_DEG) -> tuple[float, float, float, float]:
    return float(spots["lon"].min()) - margin_deg, float(spots["lon"].max()) + margin_deg, float(spots["lat"].min()) - margin_deg, float(spots["lat"].max()) + margin_deg


def download_original_files() -> None:
    log_info(f"=== Download {DATASET_ID} ===")
    copernicusmarine.get(dataset_id=DATASET_ID, output_directory=str(RAW_WAV_DIR), filter=YEAR_FILTER, overwrite=False, disable_progress_bar=False)


def find_netcdf_files(folder: Path, years: list[int] | None = None) -> list[Path]:
    files = sorted(folder.rglob("*.nc"))
    if years:
        years_as_str = {str(y) for y in years}
        files = [f for f in files if f.name[:4] in years_as_str or any(part in years_as_str for part in f.parts)]
    return files


def standardize_coords(ds: xr.Dataset) -> xr.Dataset:
    rename_map = {}
    if "latitude" in ds.coords and "lat" not in ds.coords: rename_map["latitude"] = "lat"
    if "longitude" in ds.coords and "lon" not in ds.coords: rename_map["longitude"] = "lon"
    return ds.rename(rename_map) if rename_map else ds


def crop_dataset(ds: xr.Dataset, lon_min: float, lon_max: float, lat_min: float, lat_max: float) -> xr.Dataset:
    if "lon" not in ds.coords or "lat" not in ds.coords: raise ValueError("Le dataset ne contient pas de coordonnées lat/lon")
    lat0 = float(ds["lat"].values[0]); lat1 = float(ds["lat"].values[-1])
    lon0 = float(ds["lon"].values[0]); lon1 = float(ds["lon"].values[-1])
    lat_slice = slice(lat_min, lat_max) if lat0 < lat1 else slice(lat_max, lat_min)
    lon_slice = slice(lon_min, lon_max) if lon0 < lon1 else slice(lon_max, lon_min)
    return ds.sel(lon=lon_slice, lat=lat_slice)


def pick_existing_vars(ds: xr.Dataset) -> dict[str, str]:
    picked = {}
    for out_name, aliases in VAR_CANDIDATES.items():
        for alias in aliases:
            if alias in ds.data_vars:
                picked[out_name] = alias
                break
    return picked


def open_and_crop_wav(nc_files: list[Path], lon_min: float, lon_max: float, lat_min: float, lat_max: float) -> xr.Dataset:
    if not nc_files: raise FileNotFoundError("Aucun fichier .nc trouvé pour WAV")
    datasets: list[xr.Dataset] = []
    total = len(nc_files)
    print(f"\n[WAV] {total} fichiers à ouvrir...")
    for i, file_path in enumerate(nc_files, start=1):
        t0 = time.time(); print(f"[WAV] [{i}/{total}] ouverture: {file_path.name}", flush=True)
        try:
            ds = xr.open_dataset(file_path, engine="netcdf4")
            ds = standardize_coords(ds)
            picked = pick_existing_vars(ds)
            if not picked:
                print(f"[WAV] [{i}/{total}] aucune variable utile trouvée", flush=True); ds.close(); continue
            ds = ds[list(picked.values())].rename({v: k for k, v in picked.items()})
            ds = crop_dataset(ds, lon_min, lon_max, lat_min, lat_max)
            ds.load()
            if ds.sizes.get("lat", 0) == 0 or ds.sizes.get("lon", 0) == 0:
                print(f"[WAV] [{i}/{total}] vide après découpe", flush=True); ds.close(); continue
            print(f"[WAV] [{i}/{total}] OK en {time.time() - t0:.1f}s | vars={list(ds.data_vars)} | dims={dict(ds.sizes)}", flush=True)
            datasets.append(ds)
        except Exception as exc:
            print(f"[WAV] [{i}/{total}] ERREUR en {time.time() - t0:.1f}s : {exc}", flush=True)
    if not datasets: raise ValueError("Aucun fichier exploitable pour WAV")
    print(f"[WAV] concaténation de {len(datasets)} sous-datasets...", flush=True)
    ds_merged = xr.concat(datasets, dim="time").sortby("time").drop_duplicates(dim="time")
    print(f"[WAV] concat terminé | dims={dict(ds_merged.sizes)}", flush=True)
    return ds_merged


def extract_one_spot(ds: xr.Dataset, spot: pd.Series) -> pd.DataFrame:
    point = ds.sel(lat=float(spot["lat"]), lon=float(spot["lon"]), method="nearest")
    keep_vars = [v for v in ["wave_height", "wave_period", "wave_direction"] if v in point.data_vars]
    point_df = point[keep_vars].to_dataframe().reset_index()
    if point_df.empty: return point_df
    point_df["spot_id"] = spot["spot_id"]; point_df["spot_name"] = spot["name"]
    point_df["spot_lat"] = float(spot["lat"]); point_df["spot_lon"] = float(spot["lon"])
    for col in OPTIONAL_SPOT_COLS:
        if col in spot.index: point_df[col] = spot[col]
    point_df["grid_lat"] = float(point["lat"].values); point_df["grid_lon"] = float(point["lon"].values)
    point_df["time"] = pd.to_datetime(point_df["time"]); point_df["date"] = point_df["time"].dt.floor("D")
    group_cols = ["date", "spot_id", "spot_name", "spot_lat", "spot_lon", "grid_lat", "grid_lon"]
    for col in OPTIONAL_SPOT_COLS:
        if col in point_df.columns: group_cols.append(col)
    agg_dict = {k: "mean" for k in ["wave_height", "wave_period", "wave_direction"] if k in point_df.columns}
    agg = point_df.groupby(group_cols, as_index=False).agg(agg_dict)
    if "wave_height" in agg.columns and "wave_period" in agg.columns:
        agg["wave_energy"] = (agg["wave_height"] ** 2) * agg["wave_period"]
    if "coast_orientation_deg" in agg.columns and "wave_direction" in agg.columns:
        diff = (agg["wave_direction"] - agg["coast_orientation_deg"] + 180) % 360 - 180
        agg["wave_relative_to_coast"] = diff.abs()
    agg["year"] = agg["date"].dt.year; agg["month"] = agg["date"].dt.month; agg["dayofyear"] = agg["date"].dt.dayofyear
    return agg


def build_spot_dataframe(ds: xr.Dataset, spots: pd.DataFrame) -> pd.DataFrame:
    frames = []
    total = len(spots)
    for i, (_, spot) in enumerate(spots.iterrows(), start=1):
        print(f"[SPOT] [{i}/{total}] {spot['spot_id']} ({spot['name']})", flush=True)
        df_spot = extract_one_spot(ds, spot)
        if df_spot.empty: print(f"[SPOT] [{i}/{total}] vide", flush=True); continue
        frames.append(df_spot)
    if not frames: raise ValueError("Aucune donnée spot extraite")
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    ensure_dir(RAW_WAV_DIR); ensure_dir(FEATURES_DIR)
    log_kv("RAW_DIR", RAW_WAV_DIR); log_kv("PROCESSED_DIR", FEATURES_DIR); log_kv("SPOTS_FILE", SPOTS_FILE)
    spots = load_spots(SPOTS_FILE)
    lon_min, lon_max, lat_min, lat_max = compute_bbox(spots)
    print("Spots:", len(spots)); print(f"BBox spots : lon=({lon_min:.3f}, {lon_max:.3f}) lat=({lat_min:.3f}, {lat_max:.3f})")
    nc_files = find_netcdf_files(RAW_WAV_DIR, years=YEARS)
    if not nc_files:
        if DOWNLOAD_IF_MISSING:
            print("Aucun fichier trouvé -> téléchargement Copernicus..."); download_original_files(); nc_files = find_netcdf_files(RAW_WAV_DIR, years=YEARS)
        else:
            raise FileNotFoundError(f"Aucun fichier .nc trouvé dans {RAW_WAV_DIR}. Place tes fichiers sous data/raw/wav/, ou passe DOWNLOAD_IF_MISSING=True.")
    ds_wav = open_and_crop_wav(nc_files, lon_min, lon_max, lat_min, lat_max)
    df = build_spot_dataframe(ds_wav, spots).sort_values(["spot_id", "date"]).reset_index(drop=True)
    summary_df = build_column_summary(df)
    written = save_parquet_bundle(df=df, parquet_path=OUT_FILE, sample_path=OUT_SAMPLE_FILE, summary_df=summary_df, summary_path=OUT_SUMMARY_FILE, index=False)
    print()
    for path in written.values(): log_file_written(path)
    print(); log_df_head(df, n=5)
    print("Shape:", df.shape); print("Nb spots:", df["spot_id"].nunique()); print("Date min/max:", df["date"].min(), df["date"].max())


if __name__ == "__main__":
    main()
