from __future__ import annotations

from pathlib import Path

import pandas as pd

from pscripts.supabase_client import get_supabase


def load_spots() -> pd.DataFrame:
    client = get_supabase()

    response = (
        client.table("spots")
        .select(
            "id,name,latitude_min,latitude_max,longitude_min,longitude_max,type_fond,profondeur_moyenne"
        )
        .execute()
    )

    rows = response.data or []
    if not rows:
        raise ValueError("Aucune spots trouvée dans Supabase.")

    df = pd.DataFrame(rows)

    required_cols = [
        "id",
        "name",
        "latitude_min",
        "latitude_max",
        "longitude_min",
        "longitude_max",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans la table spots: {missing}")

    df = df.dropna(
        subset=[
            "id",
            "latitude_min",
            "latitude_max",
            "longitude_min",
            "longitude_max",
        ]
    ).copy()

    df = df.rename(
        columns={
            "id": "spot_id",
            "name": "spot_name",
        }
    )

    df["latitude_min"] = pd.to_numeric(df["latitude_min"], errors="coerce")
    df["latitude_max"] = pd.to_numeric(df["latitude_max"], errors="coerce")
    df["longitude_min"] = pd.to_numeric(df["longitude_min"], errors="coerce")
    df["longitude_max"] = pd.to_numeric(df["longitude_max"], errors="coerce")
    df["profondeur_moyenne"] = pd.to_numeric(df.get("profondeur_moyenne"), errors="coerce")

    df = df.dropna(
        subset=[
            "latitude_min",
            "latitude_max",
            "longitude_min",
            "longitude_max",
        ]
    ).copy()

    df["lat_center"] = (df["latitude_min"] + df["latitude_max"]) / 2.0
    df["lon_center"] = (df["longitude_min"] + df["longitude_max"]) / 2.0

    df = df.sort_values("spot_name", na_position="last").reset_index(drop=True)
    return df


def save_spots_for_pipeline(output_path: Path) -> pd.DataFrame:
    """
    Charge les spots depuis Supabase et les sauvegarde dans un CSV au format
    attendu par le pipeline d'entraînement (colonnes: spot_id, name, lat, lon).

    Retourne le DataFrame sauvegardé.
    """
    df = load_spots()

    pipeline_df = df.rename(
        columns={
            "spot_name": "name",
            "lat_center": "lat",
            "lon_center": "lon",
        }
    ).copy()

    cols_to_keep = ["spot_id", "name", "lat", "lon"]
    for optional_col in ["type_fond", "profondeur_moyenne"]:
        if optional_col in pipeline_df.columns:
            cols_to_keep.append(optional_col)

    pipeline_df = pipeline_df[cols_to_keep]

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_df.to_csv(output_path, index=False)

    print(f"[SPOTS] {len(pipeline_df)} spots sauvegardés depuis Supabase → {output_path}")
    return pipeline_df