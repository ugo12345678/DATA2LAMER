from __future__ import annotations

import json
import os
from dataclasses import dataclass

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder

from config.settings import (
    ML_READY_FILE,
    MODELS_DIR,
    REPORTS_DIR,
    PREDICTIONS_DIR,
    MODEL_FILE,
    METRICS_FILE,
    FEATURE_IMPORTANCE_FILE,
    PREDICTIONS_FILE,
)

from scripts.upload_to_r2 import upload_file_to_r2

from src.utils.io_utils import (
    ensure_dir,
    assert_file_exists,
    read_parquet,
    write_csv,
    write_json,
    write_parquet,
)
from src.utils.logging_utils import (
    log_kv,
    log_file_written,
)


TARGET_COL = "zsd"

ID_COLS = [
    "date",
    "spot_id",
    "spot_name",
]

EXCLUDE_FROM_FEATURES = ID_COLS + [TARGET_COL]

CATEGORICAL_CANDIDATES = [
    "cluster",
]

ORDINAL_CATEGORICAL_CANDIDATES = [
    "month",
]

RANDOM_STATE = 42


@dataclass
class SplitData:
    train_df: pd.DataFrame
    valid_df: pd.DataFrame
    test_df: pd.DataFrame


def infer_feature_columns(df: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    feature_cols = [c for c in df.columns if c not in EXCLUDE_FROM_FEATURES]

    categorical_cols = [c for c in CATEGORICAL_CANDIDATES if c in feature_cols]
    ordinal_categorical_cols = [c for c in ORDINAL_CATEGORICAL_CANDIDATES if c in feature_cols]

    # éviter doublon si une colonne apparaît dans les deux listes
    ordinal_categorical_cols = [c for c in ordinal_categorical_cols if c not in categorical_cols]

    numeric_cols = [
        c for c in feature_cols
        if c not in categorical_cols and c not in ordinal_categorical_cols
    ]

    return numeric_cols, categorical_cols, ordinal_categorical_cols


def temporal_split(df: pd.DataFrame) -> SplitData:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.sort_values("date").reset_index(drop=True)

    n = len(out)
    if n == 0:
        raise ValueError("Dataset vide après chargement.")

    train_end = int(n * 0.66)
    valid_end = int(n * 0.88)

    train_df = out.iloc[:train_end].copy()
    valid_df = out.iloc[train_end:valid_end].copy()
    test_df = out.iloc[valid_end:].copy()

    if len(train_df) == 0 or len(valid_df) == 0 or len(test_df) == 0:
        raise ValueError("Split train/valid/test invalide.")

    return SplitData(train_df=train_df, valid_df=valid_df, test_df=test_df)


def build_preprocessor(
    numeric_cols: list[str],
    categorical_cols: list[str],
    ordinal_categorical_cols: list[str],
) -> ColumnTransformer:
    transformers = []

    if numeric_cols:
        numeric_pipe = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
            ]
        )
        transformers.append(("num", numeric_pipe, numeric_cols))

    if categorical_cols:
        categorical_pipe = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("onehot", OneHotEncoder(handle_unknown="ignore")),
            ]
        )
        transformers.append(("cat", categorical_pipe, categorical_cols))

    if ordinal_categorical_cols:
        ordinal_pipe = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("ordinal", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
            ]
        )
        transformers.append(("ord", ordinal_pipe, ordinal_categorical_cols))

    if not transformers:
        raise ValueError("Aucune feature exploitable trouvée.")

    preprocessor = ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        verbose_feature_names_out=True,
    )
    return preprocessor


def build_model() -> LGBMRegressor:
    return LGBMRegressor(
        objective="regression",
        n_estimators=500,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=RANDOM_STATE,
        force_col_wise=True,
    )


def compute_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    bias = float(np.mean(y_pred - y_true))
    r2 = float(r2_score(y_true, y_pred))

    return {
        "rmse": rmse,
        "mae": mae,
        "bias": bias,
        "r2": r2,
    }


def build_predictions_df(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    test_df: pd.DataFrame,
    pred_train: np.ndarray,
    pred_valid: np.ndarray,
    pred_test: np.ndarray,
) -> pd.DataFrame:
    parts = []

    for split_name, split_df, preds in [
        ("train", train_df, pred_train),
        ("valid", valid_df, pred_valid),
        ("test", test_df, pred_test),
    ]:
        tmp = split_df[["date", "spot_id", "spot_name", TARGET_COL]].copy()
        tmp["split"] = split_name
        tmp["y_true"] = tmp[TARGET_COL]
        tmp["y_pred"] = preds
        tmp["residual"] = tmp["y_pred"] - tmp["y_true"]
        tmp = tmp.drop(columns=[TARGET_COL])
        parts.append(tmp)

    out = pd.concat(parts, axis=0, ignore_index=True)
    return out


def build_feature_importance_df(
    model: LGBMRegressor,
    feature_names: list[str],
) -> pd.DataFrame:
    fi = pd.DataFrame(
        {
            "feature_transformed": feature_names,
            "importance": model.feature_importances_,
        }
    )

    def simplify_feature_group(name: str) -> str:
        if "__" in name:
            name = name.split("__", 1)[1]
        if "_" in name and name.startswith("cluster_"):
            return "cluster"
        return name

    fi["feature_group"] = fi["feature_transformed"].map(simplify_feature_group)
    fi = fi.sort_values(["importance", "feature_transformed"], ascending=[False, True]).reset_index(drop=True)
    return fi


def main() -> None:
    ensure_dir(MODELS_DIR)
    ensure_dir(REPORTS_DIR)
    ensure_dir(PREDICTIONS_DIR)

    assert_file_exists(ML_READY_FILE, label="INPUT_FILE")
    log_kv("INPUT_FILE", ML_READY_FILE)

    df = read_parquet(ML_READY_FILE, label="INPUT_FILE")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    print(f"Shape dataset: {df.shape}")
    print(f"Nb spots: {df['spot_id'].nunique()}")
    print(f"Date min/max: {df['date'].min()} -> {df['date'].max()}")

    numeric_cols, categorical_cols, ordinal_categorical_cols = infer_feature_columns(df)

    print(f"Numeric features: {len(numeric_cols)}")
    print(f"Categorical features: {len(categorical_cols) + len(ordinal_categorical_cols)}")

    splits = temporal_split(df)

    print()
    print(f"Train: {splits.train_df.shape}")
    print(f"Valid: {splits.valid_df.shape}")
    print(f"Test : {splits.test_df.shape}")

    X_train = splits.train_df.drop(columns=[TARGET_COL])
    y_train = splits.train_df[TARGET_COL].astype(float)

    X_valid = splits.valid_df.drop(columns=[TARGET_COL])
    y_valid = splits.valid_df[TARGET_COL].astype(float)

    X_test = splits.test_df.drop(columns=[TARGET_COL])
    y_test = splits.test_df[TARGET_COL].astype(float)

    preprocessor = build_preprocessor(
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
        ordinal_categorical_cols=ordinal_categorical_cols,
    )

    model = build_model()

    print("\nModel: LightGBM")

    X_train_t = preprocessor.fit_transform(X_train)
    X_valid_t = preprocessor.transform(X_valid)
    X_test_t = preprocessor.transform(X_test)

    model.fit(X_train_t, y_train)

    pred_train = model.predict(X_train_t)
    pred_valid = model.predict(X_valid_t)
    pred_test = model.predict(X_test_t)

    valid_metrics = compute_metrics(y_valid, pred_valid)
    test_metrics = compute_metrics(y_test, pred_test)

    feature_names = preprocessor.get_feature_names_out().tolist()
    fi_df = build_feature_importance_df(model, feature_names)

    predictions_df = build_predictions_df(
        train_df=splits.train_df,
        valid_df=splits.valid_df,
        test_df=splits.test_df,
        pred_train=pred_train,
        pred_valid=pred_valid,
        pred_test=pred_test,
    )

    artifact = {
        "preprocessor": preprocessor,
        "model": model,
        "target_col": TARGET_COL,
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "ordinal_categorical_cols": ordinal_categorical_cols,
        "feature_names_out": feature_names,
    }

    joblib.dump(artifact, MODEL_FILE)

    r2_model_key = os.environ.get("R2_MODEL_KEY", "lgbm_visibility_mvp.joblib")
    try:
        upload_file_to_r2(MODEL_FILE, r2_model_key)
    except Exception as exc:
        print(f"[R2] Avertissement: impossible d'uploader le modèle vers R2: {exc}")
        print(f"[R2] Le modèle reste disponible localement: {MODEL_FILE}")

    write_json(
        {
            "valid": valid_metrics,
            "test": test_metrics,
            "n_rows": len(df),
            "n_spots": int(df["spot_id"].nunique()),
            "date_min": str(df["date"].min()),
            "date_max": str(df["date"].max()),
            "numeric_features": len(numeric_cols),
            "categorical_features": len(categorical_cols) + len(ordinal_categorical_cols),
        },
        METRICS_FILE,
        indent=2,
        ensure_ascii=False,
    )
    write_csv(fi_df, FEATURE_IMPORTANCE_FILE, index=False)
    write_parquet(predictions_df, PREDICTIONS_FILE, index=False)

    print()
    log_file_written(MODEL_FILE)
    log_file_written(METRICS_FILE)
    log_file_written(FEATURE_IMPORTANCE_FILE)
    log_file_written(PREDICTIONS_FILE)

    print("\nMetrics VALID:")
    print(valid_metrics)

    print("\nMetrics TEST:")
    print(test_metrics)

    print("\nTop feature importances:")
    print(fi_df.head(20))


if __name__ == "__main__":
    main()