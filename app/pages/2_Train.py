from __future__ import annotations

from datetime import datetime

import streamlit as st

from app.services.data_loader import load_dataset, resolve_dataset_path
from app.services.feature_sets import get_feature_sets, prepare_training_frame
from app.services.train import train_and_evaluate


st.title("🧠 Train")

if "runs" not in st.session_state:
    st.session_state["runs"] = []

try:
    df = load_dataset(resolve_dataset_path())
    feature_sets = get_feature_sets(df)
except Exception as exc:
    st.error(f"Chargement impossible : {exc}")
    st.stop()

model_name = st.selectbox("Modèle", ["Ridge", "RandomForest", "LightGBM"], index=2)
feature_set_key = st.selectbox(
    "Feature set",
    options=list(feature_sets.keys()),
    format_func=lambda k: f"{feature_sets[k].label} ({len(feature_sets[k].columns)} cols)",
    index=1 if "temporal" in feature_sets else 0,
)

st.caption(feature_sets[feature_set_key].description)

params = {}
if model_name == "Ridge":
    params["alpha"] = st.slider("alpha", min_value=0.01, max_value=20.0, value=1.0, step=0.01)
elif model_name == "RandomForest":
    params["n_estimators"] = st.slider("n_estimators", min_value=100, max_value=1000, value=300, step=50)
    max_depth_choice = st.selectbox("max_depth", [None, 5, 8, 12, 20], index=0)
    params["max_depth"] = max_depth_choice
    params["min_samples_leaf"] = st.slider("min_samples_leaf", min_value=1, max_value=10, value=1, step=1)
else:
    params["n_estimators"] = st.slider("n_estimators", min_value=100, max_value=1200, value=400, step=50)
    params["learning_rate"] = st.slider("learning_rate", min_value=0.01, max_value=0.30, value=0.03, step=0.01)
    params["num_leaves"] = st.slider("num_leaves", min_value=15, max_value=127, value=31, step=4)
    params["subsample"] = st.slider("subsample", min_value=0.5, max_value=1.0, value=0.9, step=0.05)
    params["colsample_bytree"] = st.slider("colsample_bytree", min_value=0.5, max_value=1.0, value=0.9, step=0.05)

train_df, selected_features = prepare_training_frame(df, feature_set_key)

with st.expander("Colonnes utilisées", expanded=False):
    st.write(selected_features)

if st.button("Lancer l'entraînement", type="primary", use_container_width=True):
    with st.spinner("Entraînement en cours..."):
        try:
            result = train_and_evaluate(
                df=train_df,
                feature_cols=selected_features,
                model_name=model_name,
                params=params,
            )
        except Exception as exc:
            st.error(f"Échec entraînement : {exc}")
            st.stop()

    run = {
        "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "model_name": model_name,
        "feature_set_key": feature_set_key,
        "feature_set_label": feature_sets[feature_set_key].label,
        "params": params,
        "feature_count": len(selected_features),
        "metrics": result["metrics"],
        "feature_importance_df": result["feature_importance_df"],
        "predictions_df": result["predictions_df"],
    }
    st.session_state["runs"].append(run)

    st.success(f"Run {run['run_id']} ajouté.")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### VALID")
        st.json(run["metrics"]["valid"])
    with c2:
        st.markdown("### TEST")
        st.json(run["metrics"]["test"])

    st.markdown("### Top feature importances")
    st.dataframe(run["feature_importance_df"].head(20), use_container_width=True)
