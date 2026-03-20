from __future__ import annotations

import pandas as pd

from pscripts.cmems_runtime import add_zone_metadata, base_group_cols, open_cmems_dataset


DATASET_ID = "cmems_mod_ibi_phy_anfc_0.027deg-3D_P1D-m"

VAR_MAP = {
    "sst": ["thetao", "bottomT", "tos"],
    "salinity": ["so", "sos"],
    "current_u": ["uo", "vozocrtx"],
    "current_v": ["vo", "vomecrty"],
}


def _pick_available_vars(ds) -> dict[str, str]:
    picked = {}
    for target_name, aliases in VAR_MAP.items():
        for alias in aliases:
            if alias in ds.data_vars:
                picked[target_name] = alias
                break
    return picked


def fetch_phy_forecast(zones: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for _, zone in zones.iterrows():
        ds = open_cmems_dataset(
            dataset_id=DATASET_ID,
            variables=None,
            zone=zone,
            select_surface=True,
        )
        print("PHY data_vars:", list(ds.data_vars))
        picked = _pick_available_vars(ds)
        if not picked:
            continue

        ds = ds[list(picked.values())].rename({v: k for k, v in picked.items()})
        point = ds.sel(lat=float(zone["lat_center"]), lon=float(zone["lon_center"]), method="nearest")

        keep_vars = [c for c in ["sst", "salinity", "current_u", "current_v"] if c in point.data_vars]
        if not keep_vars:
            continue

        df = point[keep_vars].to_dataframe().reset_index()
        if df.empty:
            continue

        df = add_zone_metadata(df, zone, point=point)
        group_cols = base_group_cols(df)
        agg = df.groupby(group_cols, as_index=False).agg({col: "mean" for col in keep_vars})

        if "current_u" in agg.columns and "current_v" in agg.columns:
            agg["current_speed"] = (agg["current_u"] ** 2 + agg["current_v"] ** 2) ** 0.5

        frames.append(agg)

    if not frames:
        raise ValueError("Aucune donnée PHY forecast produite")

    return pd.concat(frames, ignore_index=True).sort_values(["zone_id", "date"]).reset_index(drop=True)