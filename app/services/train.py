from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder

try:
    from lightgbm import LGBMRegressor
except Exception:  # pragma: no cover
    LGBMRegressor = None

from services.feature_sets import CATEGORICAL_COLS, ID_COLS, ORDINAL_COLS, TARGET_COL


RANDOM_STATE = 42


@dataclass
class SplitData:
    train_df: pd.DataFrame
    valid_df: pd.DataFrame
    test_df: pd.DataFrame



def temporal_split(df: pd.DataFrame) -> SplitData:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.sort_values("date").reset_index(drop=True)

    n = len(out)
    if n < 30:
        raise ValueError("Dataset trop petit pour un split temporel propre.")

    train_end = int(n * 0.66)
    valid_end = int(n * 0.88)

    train_df = out.iloc[:train_end].copy()
    valid_df = out.iloc[train_end:valid_end].copy()
    test_df = out.iloc[valid_end:].copy()

    if min(len(train_df), len(valid_df), len(test_df)) == 0:
        raise ValueError("Split train/valid/test invalide.")

    return SplitData(train_df=train_df, valid_df=valid_df, test_df=test_df)



def infer_column_types(df: pd.DataFrame, feature_cols: list[str]) -> tuple[list[str], list[str], list[str]]:
    categorical_cols = [c for c in CATEGORICAL_COLS if c in feature_cols]
    ordinal_cols = [c for c in ORDINAL_COLS if c in feature_cols and c not in categorical_cols]
    numeric_cols = [c for c in feature_cols if c not in categorical_cols and c not in ordinal_cols]
    return numeric_cols, categorical_cols, ordinal_cols



def build_preprocessor(
    numeric_cols: list[str],
    categorical_cols: list[str],
    ordinal_cols: list[str],
) -> ColumnTransformer:
    transformers: list[tuple[str, Pipeline, list[str]]] = []

    if numeric_cols:
        transformers.append(
            (
                "num",
                Pipeline([("imputer", SimpleImputer(strategy="median"))]),
                numeric_cols,
            )
        )

    if categorical_cols:
        transformers.append(
            (
                "cat",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_cols,
            )
        )

    if ordinal_cols:
        transformers.append(
            (
                "ord",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "ordinal",
                            OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                        ),
                    ]
                ),
                ordinal_cols,
            )
        )

    if not transformers:
        raise ValueError("Aucune feature exploitable trouvée.")

    return ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        verbose_feature_names_out=True,
    )



def build_model(model_name: str, params: dict[str, Any] | None = None):
    params = params or {}

    if model_name == "Ridge":
        alpha = float(params.get("alpha", 1.0))
        return Ridge(alpha=alpha)

    if model_name == "RandomForest":
        return RandomForestRegressor(
            n_estimators=int(params.get("n_estimators", 300)),
            max_depth=params.get("max_depth"),
            min_samples_leaf=int(params.get("min_samples_leaf", 1)),
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )

    if model_name == "LightGBM":
        if LGBMRegressor is None:
            raise ImportError("lightgbm n'est pas installé dans cet environnement.")
        return LGBMRegressor(
            objective="regression",
            n_estimators=int(params.get("n_estimators", 400)),
            learning_rate=float(params.get("learning_rate", 0.03)),
            num_leaves=int(params.get("num_leaves", 31)),
            subsample=float(params.get("subsample", 0.9)),
            colsample_bytree=float(params.get("colsample_bytree", 0.9)),
            random_state=RANDOM_STATE,
            force_col_wise=True,
        )

    raise KeyError(f"Modèle inconnu: {model_name}")



def compute_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "bias": float(np.mean(y_pred - y_true)),
        "r2": float(r2_score(y_true, y_pred)),
    }



def _build_feature_importance_df(model, feature_names: list[str], model_name: str) -> pd.DataFrame:
    if model_name in {"RandomForest", "LightGBM"} and hasattr(model, "feature_importances_"):
        importances = np.asarray(model.feature_importances_, dtype=float)
    elif model_name == "Ridge" and hasattr(model, "coef_"):
        importances = np.abs(np.asarray(model.coef_, dtype=float).ravel())
    else:
        importances = np.zeros(len(feature_names), dtype=float)

    fi = pd.DataFrame({
        "feature_transformed": feature_names,
        "importance": importances,
    })

    def simplify(name: str) -> str:
        base = name.split("__", 1)[1] if "__" in name else name
        if base.startswith("cluster_"):
            return "cluster"
        return base

    fi["feature_group"] = fi["feature_transformed"].map(simplify)
    fi = fi.sort_values(["importance", "feature_transformed"], ascending=[False, True]).reset_index(drop=True)
    return fi



def train_and_evaluate(
    df: pd.DataFrame,
    feature_cols: list[str],
    model_name: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    splits = temporal_split(df)
    numeric_cols, categorical_cols, ordinal_cols = infer_column_types(df, feature_cols)

    preprocessor = build_preprocessor(numeric_cols, categorical_cols, ordinal_cols)
    estimator = build_model(model_name, params=params)

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", estimator),
        ]
    )

    X_train = splits.train_df[feature_cols]
    y_train = splits.train_df[TARGET_COL]
    X_valid = splits.valid_df[feature_cols]
    y_valid = splits.valid_df[TARGET_COL]
    X_test = splits.test_df[feature_cols]
    y_test = splits.test_df[TARGET_COL]

    pipeline.fit(X_train, y_train)

    pred_train = pipeline.predict(X_train)
    pred_valid = pipeline.predict(X_valid)
    pred_test = pipeline.predict(X_test)

    metrics = {
        "train": compute_metrics(y_train, pred_train),
        "valid": compute_metrics(y_valid, pred_valid),
        "test": compute_metrics(y_test, pred_test),
    }

    transformed_names = pipeline.named_steps["preprocessor"].get_feature_names_out().tolist()
    model = pipeline.named_steps["model"]
    feature_importance_df = _build_feature_importance_df(model, transformed_names, model_name)

    predictions_df = pd.concat(
        [
            _build_predictions_frame(splits.train_df, pred_train, "train"),
            _build_predictions_frame(splits.valid_df, pred_valid, "valid"),
            _build_predictions_frame(splits.test_df, pred_test, "test"),
        ],
        ignore_index=True,
    )

    return {
        "pipeline": pipeline,
        "splits": splits,
        "metrics": metrics,
        "feature_importance_df": feature_importance_df,
        "predictions_df": predictions_df,
        "feature_cols": feature_cols,
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "ordinal_cols": ordinal_cols,
    }



def _build_predictions_frame(df: pd.DataFrame, preds: np.ndarray, split_name: str) -> pd.DataFrame:
    cols = [c for c in ID_COLS if c in df.columns]
    out = df[cols].copy()
    out[TARGET_COL] = df[TARGET_COL].values
    out["y_pred"] = preds
    out["residual"] = out["y_pred"] - out[TARGET_COL]
    out["split"] = split_name
    return out
