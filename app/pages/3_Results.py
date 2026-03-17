from __future__ import annotations

import pandas as pd
import streamlit as st


st.title("📊 Results")

runs = st.session_state.get("runs", [])
if not runs:
    st.info("Aucun run disponible. Lance d'abord un entraînement depuis la page Train.")
    st.stop()

comparison_rows = []
for run in runs:
    comparison_rows.append(
        {
            "run_id": run["run_id"],
            "created_at": run["created_at"],
            "model": run["model_name"],
            "feature_set": run["feature_set_label"],
            "feature_count": run["feature_count"],
            "valid_rmse": run["metrics"]["valid"]["rmse"],
            "valid_mae": run["metrics"]["valid"]["mae"],
            "valid_bias": run["metrics"]["valid"]["bias"],
            "valid_r2": run["metrics"]["valid"]["r2"],
            "test_rmse": run["metrics"]["test"]["rmse"],
            "test_mae": run["metrics"]["test"]["mae"],
            "test_bias": run["metrics"]["test"]["bias"],
            "test_r2": run["metrics"]["test"]["r2"],
        }
    )

comparison_df = pd.DataFrame(comparison_rows).sort_values("test_rmse", ascending=True).reset_index(drop=True)

st.markdown("### Comparaison des runs")
st.dataframe(comparison_df, use_container_width=True, height=320)

selected_run_id = st.selectbox("Sélectionner un run", comparison_df["run_id"].tolist(), index=0)
selected_run = next(run for run in runs if run["run_id"] == selected_run_id)

left, right = st.columns(2)
with left:
    st.markdown("### VALID")
    st.json(selected_run["metrics"]["valid"])
with right:
    st.markdown("### TEST")
    st.json(selected_run["metrics"]["test"])

st.markdown("### Top 30 feature importances")
fi_df = selected_run["feature_importance_df"].head(30).copy()
st.dataframe(fi_df, use_container_width=True, height=500)

st.markdown("### Résidus sur le split test")
preds_df = selected_run["predictions_df"]
test_df = preds_df[preds_df["split"] == "test"].copy()
if not test_df.empty:
    st.line_chart(test_df[["y_pred", "zsd"]].reset_index(drop=True))
    st.dataframe(test_df.head(200), use_container_width=True, height=300)

export_csv = comparison_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Télécharger le tableau de comparaison",
    data=export_csv,
    file_name="runs_comparison.csv",
    mime="text/csv",
)
