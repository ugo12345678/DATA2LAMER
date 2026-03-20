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


def sanitize_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    return v


def sanitize_records(df: pd.DataFrame) -> list[dict]:
    records = []
    for row in df.to_dict(orient="records"):
        clean = {k: sanitize_value(v) for k, v in row.items()}
        records.append(clean)
    return records


def resolve_model_artifact(artifact):
    if not isinstance(artifact, dict):
        raise TypeError(
            f"Artefact inattendu: {type(artifact)}. "
            "Le .joblib attendu doit être un dict contenant preprocessor et model."
        )

    required_keys = [
        "preprocessor",
        "model",
        "numeric_cols",
        "categorical_cols",
        "ordinal_categorical_cols",
    ]
    missing_keys = [k for k in required_keys if k not in artifact]
    if missing_keys:
        raise KeyError(f"Clés manquantes dans l'artefact modèle: {missing_keys}")

    preprocessor = artifact["preprocessor"]
    model = artifact["model"]
    raw_feature_cols = (
        list(artifact["numeric_cols"])
        + list(artifact["categorical_cols"])
        + list(artifact["ordinal_categorical_cols"])
    )

    return preprocessor, model, raw_feature_cols


def prepare_inference_matrix(features_df: pd.DataFrame, raw_feature_cols: list[str]) -> pd.DataFrame:
    missing_cols = [c for c in raw_feature_cols if c not in features_df.columns]
    if missing_cols:
        raise ValueError(f"Features manquantes pour l'inférence: {missing_cols}")

    X_raw = features_df[raw_feature_cols].copy()

    print(f"Nb features brutes attendues: {len(raw_feature_cols)}")
    print(f"Shape X_raw: {X_raw.shape}")

    nan_counts = X_raw.isna().sum()
    nan_counts = nan_counts[nan_counts > 0]
    if not nan_counts.empty:
        print("NaN par colonne avant preprocessing:")
        print(nan_counts.to_dict())

    return X_raw


def build_prediction_rows(df: pd.DataFrame, preds) -> list[dict]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    forecast_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()

    rows = []
    feature_snapshots = sanitize_records(df.drop(columns=["zone_name"], errors="ignore"))

    for i, (_, row) in enumerate(df.iterrows()):
        target_time = pd.to_datetime(row["date"], errors="coerce", utc=True)
        rows.append(
            {
                "zone_id": row["zone_id"],
                "forecast_time": forecast_time,
                "target_time": target_time.isoformat() if pd.notna(target_time) else None,
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

    rows = [r for r in rows if r["zone_id"] is not None and r["target_time"] is not None]
    if not rows:
        print("[WARN] aucune ligne valide à upserter.")
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
    print(f"Feature frame cols: {len(features_df.columns)}")

    artifact = load_model_from_r2()
    print("Type artefact chargé:", type(artifact))
    if isinstance(artifact, dict):
        print("Clés artefact:", list(artifact.keys()))

    preprocessor, model, raw_feature_cols = resolve_model_artifact(artifact)

    X_raw = prepare_inference_matrix(features_df, raw_feature_cols)
    X_t = preprocessor.transform(X_raw)

    print(f"Shape X_transformed: {X_t.shape}")
    preds = model.predict(X_t)

    rows = build_prediction_rows(features_df, preds)
    upsert_predictions(rows)

    print(f"[OK] rows upsertées: {len(rows)}")
    print(json.dumps(rows[:2], indent=2, default=str))


if __name__ == "__main__":
    main()