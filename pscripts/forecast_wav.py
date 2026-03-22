from __future__ import annotations

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from pscripts.cmems_runtime import add_zone_metadata, base_group_cols, open_cmems_dataset


DATASET_ID = "cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i"
CMEMS_MAX_WORKERS = int(os.environ.get("CMEMS_MAX_WORKERS", "8"))

VAR_MAP = {
    "wave_height": ["VHM0", "swh", "hm0"],
    "wave_period": ["VTM10", "mwp", "tm10"],
    "wave_direction": ["VMDR", "mwd"],
}


def _pick_available_vars(ds) -> dict[str, str]:
    picked = {}
    for target_name, aliases in VAR_MAP.items():
        for alias in aliases:
            if alias in ds.data_vars:
                picked[target_name] = alias
                break
    return picked


def _fetch_wav_zone(zone: pd.Series) -> pd.DataFrame | None:
    ds = open_cmems_dataset(
        dataset_id=DATASET_ID,
        variables=None,
        zone=zone,
        select_surface=False,
    )
    picked = _pick_available_vars(ds)
    if not picked:
        return None

    ds = ds[list(picked.values())].rename({v: k for k, v in picked.items()})
    point = ds.sel(lat=float(zone["lat_center"]), lon=float(zone["lon_center"]), method="nearest")

    keep_vars = [c for c in ["wave_height", "wave_period", "wave_direction"] if c in point.data_vars]
    if not keep_vars:
        return None

    df = point[keep_vars].to_dataframe().reset_index()
    if df.empty:
        return None

    df = add_zone_metadata(df, zone, point=point)
    group_cols = base_group_cols(df)
    agg = df.groupby(group_cols, as_index=False).agg({col: "mean" for col in keep_vars})

    if "wave_height" in agg.columns and "wave_period" in agg.columns:
        agg["wave_energy"] = (agg["wave_height"] ** 2) * agg["wave_period"]

    return agg


def fetch_wav_forecast(zones: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=CMEMS_MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_wav_zone, zone): zone["zone_id"] for _, zone in zones.iterrows()}

        for future in as_completed(futures):
            zone_id = futures[future]
            try:
                result = future.result()
                if result is not None:
                    frames.append(result)
            except Exception as exc:
                print(f"[WARN] Échec récupération WAV pour zone {zone_id}: {exc}")

    if not frames:
        raise ValueError("Aucune donnée WAV forecast produite")

    return pd.concat(frames, ignore_index=True).sort_values(["zone_id", "date"]).reset_index(drop=True)