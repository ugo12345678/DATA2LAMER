from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd

from pscripts.feature_builder import build_feature_frame
from pscripts.forecast_bgc import fetch_bgc_forecast
from pscripts.forecast_meteo import fetch_meteo_forecast
from pscripts.forecast_phy import fetch_phy_forecast
from pscripts.forecast_wav import fetch_wav_forecast
from pscripts.model_loader import load_model_from_r2
from pscripts.supabase_client import get_supabase
from pscripts.zones import load_zones


MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")
PREDICTION_SOURCE = os.environ.get("PREDICTION_SOURCE", "github_action")

NON_FEATURE_COLS = {
    "zone_id",
    "zone_name",
    "date",
    "latitude_min",
    "latitude_max",
    "longitude_min",
    "longitude_max",
    "lat_center",
    "lon_center",
    "grid_lat",
    "grid_lon",
}


def infer_feature_columns(df: pd.DataFrame, model) -> list[str]:
    if hasattr(model, "feature_names_in_"):
        cols = [c for c in model.feature_names_in_ if c in df.columns]
        if cols:
            return cols

    excluded = NON_FEATURE_COLS.copy()
    return [c for c in df.columns if c not in excluded]


def sanitize_records(df: pd.DataFrame) -> list[dict]:
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if pd.isna(v):
                clean[k] = None
            elif hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        records.append(clean)
    return records


def build_prediction_rows(df: pd.DataFrame, preds) -> list[dict]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    forecast_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()

    rows = []
    feature_snapshots = sanitize_records(df.drop(columns=["zone_name"], errors="ignore"))

    for i, (_, row) in enumerate(df.iterrows()):
        rows.append(
            {
                "zone_id": row["zone_id"],
                "forecast_time": forecast_time,
                "target_time": pd.to_datetime(row["date"]).isoformat(),
                "pred_visibility": float(preds[i]),
                "model_version": MODEL_VERSION,
                "run_id": run_id,
                "source": PREDICTION_SOURCE,
                "features_json": feature_snapshots[i],
            }
        )
    return rows


def upsert_predictions(rows: list[dict]) -> None:
    if not rows:
        return

    client = get_supabase()
    (
        client.table("forecast_predictions")
        .upsert(rows, on_conflict="zone_id,target_time,model_version")
        .execute()
    )


def main() -> None:
    zones = load_zones()
    print(f"Zones chargées: {len(zones)}")

    phy_df = fetch_phy_forecast(zones)
    print(f"PHY rows: {len(phy_df)}")

    wav_df = fetch_wav_forecast(zones)
    print(f"WAV rows: {len(wav_df)}")

    bgc_df = fetch_bgc_forecast(zones)
    print(f"BGC rows: {len(bgc_df)}")

    meteo_df = fetch_meteo_forecast(zones)
    print(f"METEO rows: {len(meteo_df)}")

    features_df = build_feature_frame(
        phy_df=phy_df,
        wav_df=wav_df,
        bgc_df=bgc_df,
        meteo_df=meteo_df,
    )
    print(f"Feature frame rows: {len(features_df)}")

    model = load_model_from_r2()
    feature_cols = infer_feature_columns(features_df, model)
    print(f"Nb features utilisées: {len(feature_cols)}")

    X = features_df[feature_cols].copy()
    preds = model.predict(X)

    rows = build_prediction_rows(features_df, preds)
    upsert_predictions(rows)

    print(f"[OK] rows upsertées: {len(rows)}")
    print(json.dumps(rows[:2], indent=2, default=str))


if __name__ == "__main__":
    main()