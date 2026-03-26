from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd

from pscripts.cmems_runtime import forecast_today
from pscripts.feature_builder import build_feature_frame
from pscripts.forecast_bgc import fetch_bgc_forecast
from pscripts.forecast_meteo import fetch_meteo_forecast
from pscripts.forecast_phy import fetch_phy_forecast
from pscripts.forecast_wav import fetch_wav_forecast
from pscripts.model_loader import load_model_from_r2
from pscripts.supabase_client import get_supabase
from pscripts.spots import load_spots


MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")
PREDICTION_SOURCE = os.environ.get("PREDICTION_SOURCE", "github_action")
FORECAST_THREAD_WORKERS = int(os.environ.get("FORECAST_THREAD_WORKERS", "4"))


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


def compute_data_completeness(X_raw: pd.DataFrame) -> pd.Series:
    """Ratio of non-NaN features per row (0.0 to 1.0)."""
    total = X_raw.shape[1]
    if total == 0:
        return pd.Series(0.0, index=X_raw.index)
    return X_raw.notna().sum(axis=1) / total


def build_prediction_rows(
    df: pd.DataFrame, preds, completeness: pd.Series,
) -> list[dict]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    forecast_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()

    rows = []
    feature_snapshots = sanitize_records(df.drop(columns=["spot_name"], errors="ignore"))

    for i, (_, row) in enumerate(df.iterrows()):
        target_time = pd.to_datetime(row["date"], errors="coerce", utc=True)
        rows.append(
            {
                "spot_id": row["spot_id"],
                "forecast_time": forecast_time,
                "target_time": target_time.isoformat() if pd.notna(target_time) else None,
                "pred_visibility": float(preds[i]),
                "model_version": MODEL_VERSION,
                "run_id": run_id,
                "source": PREDICTION_SOURCE,
                "data_completeness": round(float(completeness.iloc[i]), 4),
                "features_json": feature_snapshots[i],
            }
        )
    return rows


def delete_past_forecasts() -> None:
    """Delete forecast predictions with a target_time before today."""
    client = get_supabase()
    today_utc = forecast_today().isoformat()
    resp = (
        client.table("forecast_predictions")
        .delete()
        .lt("target_time", today_utc)
        .execute()
    )
    deleted = len(resp.data) if resp.data else 0
    print(f"[CLEANUP] {deleted} anciennes prédictions supprimées (target_time < {today_utc})")


def upsert_predictions(rows: list[dict]) -> None:
    if not rows:
        return

    rows = [r for r in rows if r["spot_id"] is not None and r["target_time"] is not None]
    if not rows:
        print("[WARN] aucune ligne valide à upserter.")
        return

    client = get_supabase()

    # Fetch existing completeness for the same keys to avoid overwriting better data
    target_times = sorted({r["target_time"] for r in rows})
    t_min, t_max = target_times[0], target_times[-1]

    # Query existing predictions using date range only (spot_ids list too large for URL)
    existing = (
        client.table("forecast_predictions")
        .select("spot_id,target_time,model_version,data_completeness")
        .gte("target_time", t_min)
        .lte("target_time", t_max)
        .eq("model_version", MODEL_VERSION)
        .limit(10000)
        .execute()
    )

    existing_map: dict[tuple, float] = {}
    for e in existing.data or []:
        key = (e["spot_id"], e["target_time"], e["model_version"])
        existing_map[key] = e.get("data_completeness") or 0.0

    rows_to_upsert = []
    skipped = 0
    for r in rows:
        key = (r["spot_id"], r["target_time"], r["model_version"])
        prev = existing_map.get(key)
        if prev is not None and r["data_completeness"] < prev:
            skipped += 1
            continue
        rows_to_upsert.append(r)

    if skipped:
        print(f"[INFO] {skipped} lignes ignorées (completeness inférieure à l'existant)")

    if not rows_to_upsert:
        print("[INFO] Aucune ligne à upserter (données existantes déjà meilleures).")
        return

    # Upsert par batch pour éviter les limites de taille de requête
    BATCH_SIZE = 500
    total_upserted = 0
    for i in range(0, len(rows_to_upsert), BATCH_SIZE):
        batch = rows_to_upsert[i : i + BATCH_SIZE]
        (
            client.table("forecast_predictions")
            .upsert(batch, on_conflict="spot_id,target_time,model_version")
            .execute()
        )
        total_upserted += len(batch)

    print(f"[OK] {total_upserted} lignes upsertées sur {len(rows)} candidates ({total_upserted // BATCH_SIZE + 1} batch(es))")


def main() -> None:
    spots = load_spots()
    print(f"Spots chargées: {len(spots)}")

    # Load model in parallel with data fetching for speed
    with ThreadPoolExecutor(max_workers=FORECAST_THREAD_WORKERS + 1) as executor:
        model_future = executor.submit(load_model_from_r2)

        futures = {
            executor.submit(fetch_phy_forecast, spots): "phy",
            executor.submit(fetch_wav_forecast, spots): "wav",
            executor.submit(fetch_bgc_forecast, spots): "bgc",
            executor.submit(fetch_meteo_forecast, spots): "meteo",
        }

        phy_df = wav_df = bgc_df = meteo_df = pd.DataFrame()

        for future in as_completed(futures):
            source = futures[future]
            try:
                result = future.result()
                if source == "phy":
                    phy_df = result
                    print(f"PHY rows: {len(phy_df)}")
                elif source == "wav":
                    wav_df = result
                    print(f"WAV rows: {len(wav_df)}")
                elif source == "bgc":
                    bgc_df = result
                    print(f"BGC rows: {len(bgc_df)}")
                elif source == "meteo":
                    meteo_df = result
                    print(f"METEO rows: {len(meteo_df)}")
            except Exception as exc:
                print(f"[ERROR] Échec récupération {source.upper()} forecast: {exc}")

        artifact = model_future.result()

    if phy_df.empty or wav_df.empty or bgc_df.empty or meteo_df.empty:
        raise RuntimeError("Un des jeux de données forecast est vide après récupération (échec possible).")

    features_df = build_feature_frame(
        phy_df=phy_df,
        wav_df=wav_df,
        bgc_df=bgc_df,
        meteo_df=meteo_df,
    )
    print(f"Feature frame rows: {len(features_df)}")
    print(f"Feature frame cols: {len(features_df.columns)}")

    print("Type artefact chargé:", type(artifact))
    if isinstance(artifact, dict):
        print("Clés artefact:", list(artifact.keys()))

    preprocessor, model, raw_feature_cols = resolve_model_artifact(artifact)

    X_raw = prepare_inference_matrix(features_df, raw_feature_cols)
    completeness = compute_data_completeness(X_raw)
    X_t = preprocessor.transform(X_raw)

    print(f"Shape X_transformed: {X_t.shape}")
    print(f"Data completeness: min={completeness.min():.2%} mean={completeness.mean():.2%} max={completeness.max():.2%}")
    preds = model.predict(X_t)

    rows = build_prediction_rows(features_df, preds, completeness)
    delete_past_forecasts()
    upsert_predictions(rows)

    print(f"[OK] rows upsertées: {len(rows)}")
    print(json.dumps(rows[:2], indent=2, default=str))


if __name__ == "__main__":
    main()