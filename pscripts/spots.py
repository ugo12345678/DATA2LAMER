from __future__ import annotations

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