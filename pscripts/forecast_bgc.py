from __future__ import annotations

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from pscripts.cmems_runtime import add_spot_metadata, base_group_cols, open_cmems_dataset


DATASET_ID = "cmems_mod_ibi_bgc_anfc_0.027deg-3D_P1D-m"
CMEMS_MAX_WORKERS = int(os.environ.get("CMEMS_MAX_WORKERS", "8"))

VAR_MAP = {
    "chl_model": ["chl", "CHL", "chl1", "chlorophyll"],
    "light_attenuation": ["kd", "kd490", "KD490", "att", "light_attenuation", "mldr10_1"],
    "phyc": ["phyc", "PHY", "phytoplankton_carbon"],
}


def _pick_available_vars(ds) -> dict[str, str]:
    picked = {}
    for target_name, aliases in VAR_MAP.items():
        for alias in aliases:
            if alias in ds.data_vars:
                picked[target_name] = alias
                break
    return picked


def _fetch_bgc_spot(spot: pd.Series) -> pd.DataFrame | None:
    from datetime import timedelta
    from pscripts.cmems_runtime import forecast_today
    today = forecast_today()
    end_want = today + timedelta(days=4)
    ds = open_cmems_dataset(
        dataset_id=DATASET_ID,
        variables=None,
        spot=spot,
        select_surface=True,
        start_datetime=today.isoformat(),
        end_datetime=end_want.isoformat(),
    )
    picked = _pick_available_vars(ds)
    if not picked:
        return None

    ds = ds[list(picked.values())].rename({v: k for k, v in picked.items()})
    point = ds.sel(lat=float(spot["lat_center"]), lon=float(spot["lon_center"]), method="nearest")

    keep_vars = [c for c in ["chl_model", "light_attenuation", "phyc"] if c in point.data_vars]
    if not keep_vars:
        return None

    df = point[keep_vars].to_dataframe().reset_index()
    if df.empty:
        return None

    df = add_spot_metadata(df, spot, point=point)
    group_cols = base_group_cols(df)
    agg = df.groupby(group_cols, as_index=False).agg({col: "mean" for col in keep_vars})

    return agg


def fetch_bgc_forecast(spots: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=CMEMS_MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_bgc_spot, spot): spot["spot_id"] for _, spot in spots.iterrows()}

        for future in as_completed(futures):
            spot_id = futures[future]
            try:
                result = future.result()
                if result is not None:
                    frames.append(result)
            except Exception as exc:
                print(f"[WARN] Échec récupération BGC pour spot {spot_id}: {exc}")

    if not frames:
        raise ValueError("Aucune donnée BGC forecast produite")

    return pd.concat(frames, ignore_index=True).sort_values(["spot_id", "date"]).reset_index(drop=True)