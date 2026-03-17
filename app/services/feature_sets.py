from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


TARGET_COL = "zsd"
ID_COLS = ["date", "spot_id", "spot_name"]
CATEGORICAL_COLS = ["cluster"]
ORDINAL_COLS = ["month"]

BASE_FEATURES = [
    "spot_lat",
    "spot_lon",
    "grid_lat",
    "grid_lon",
    "coast_orientation_deg",
    "year",
    "month",
    "dayofyear",
    "sst",
    "salinity",
    "current_u",
    "current_v",
    "current_speed",
    "wave_height",
    "wave_period",
    "wave_direction",
    "wave_energy",
    "wave_relative_to_coast",
    "chl_model",
    "phyc",
    "wind_speed",
    "wind_direction",
    "wind_gusts",
    "rain_24h",
    "rain_48h",
    "rain_72h",
    "cluster",
]

TEMPORAL_FEATURES = [
    "wave_height_lag_1",
    "wave_energy_lag_1",
    "wave_period_lag_1",
    "rain_24h_lag_1",
    "rain_48h_lag_1",
    "rain_72h_lag_1",
    "current_speed_lag_1",
    "chl_model_lag_1",
    "sst_lag_1",
    "wave_height_roll3_mean",
    "wave_energy_roll3_mean",
    "current_speed_roll3_mean",
    "sst_roll3_mean",
]

AUTOREGRESSIVE_FEATURES = [
    "zsd_lag_1",
]


@dataclass(frozen=True)
class FeatureSet:
    key: str
    label: str
    columns: list[str]
    description: str



def _available(df: pd.DataFrame, cols: list[str]) -> list[str]:
    return [c for c in cols if c in df.columns]



def get_feature_sets(df: pd.DataFrame) -> dict[str, FeatureSet]:
    base = _available(df, BASE_FEATURES)
    temporal = base + _available(df, TEMPORAL_FEATURES)
    temporal_plus_target = temporal + _available(df, AUTOREGRESSIVE_FEATURES)

    return {
        "base": FeatureSet(
            key="base",
            label="Base",
            columns=base,
            description="Variables météo, océano et calendrier, sans lags.",
        ),
        "temporal": FeatureSet(
            key="temporal",
            label="Temporal",
            columns=temporal,
            description="Base + lags/rolling déjà calculés dans ton pipeline.",
        ),
        "temporal_plus_zsd_lag_1": FeatureSet(
            key="temporal_plus_zsd_lag_1",
            label="Temporal + zsd_lag_1",
            columns=temporal_plus_target,
            description="Temporal + feature autorégressive si disponible.",
        ),
    }



def prepare_training_frame(df: pd.DataFrame, feature_set_key: str) -> tuple[pd.DataFrame, list[str]]:
    feature_sets = get_feature_sets(df)
    if feature_set_key not in feature_sets:
        raise KeyError(f"Feature set inconnu: {feature_set_key}")

    selected_features = feature_sets[feature_set_key].columns
    required_cols = [TARGET_COL] + [c for c in ID_COLS if c in df.columns]
    selected_cols = list(dict.fromkeys(required_cols + selected_features))

    out = df[selected_cols].copy()
    out = out.dropna(subset=[TARGET_COL]).reset_index(drop=True)
    return out, selected_features
