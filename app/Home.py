from __future__ import annotations

import streamlit as st

from app.services.data_loader import build_dataset_info, load_dataset, resolve_dataset_path
from app.services.feature_sets import get_feature_sets


st.set_page_config(page_title="Bretagne Visibility ML", page_icon="🌊", layout="wide")

st.title("🌊 Bretagne Visibility ML")
st.caption("V1 Streamlit pour explorer le dataset et lancer des entraînements depuis l'interface.")

if "runs" not in st.session_state:
    st.session_state["runs"] = []

try:
    dataset_path = resolve_dataset_path()
    df = load_dataset(dataset_path)
    info = build_dataset_info(df, dataset_path)
    feature_sets = get_feature_sets(df)
except Exception as exc:
    st.error(f"Impossible de charger le dataset : {exc}")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Lignes", f"{info.shape[0]:,}".replace(",", " "))
col2.metric("Colonnes", info.shape[1])
col3.metric("Spots", info.n_spots)
col4.metric("Runs comparés", len(st.session_state["runs"]))

st.markdown("### Dataset détecté")
st.code(str(info.path))

left, right = st.columns([1.2, 1])
with left:
    st.markdown("### Périmètre")
    st.write(
        {
            "date_min": str(info.date_min.date()) if info.date_min is not None and not hasattr(info.date_min, 'tz') else str(info.date_min),
            "date_max": str(info.date_max.date()) if info.date_max is not None and not hasattr(info.date_max, 'tz') else str(info.date_max),
            "numeric_cols": len(info.numeric_cols),
            "categorical_cols": len(info.categorical_cols),
        }
    )

with right:
    st.markdown("### Feature sets disponibles")
    for fs in feature_sets.values():
        st.markdown(f"**{fs.label}** — {len(fs.columns)} colonnes")
        st.caption(fs.description)

st.markdown("### Navigation")
st.info(
    "Dataset : inspection des colonnes, filtre par spot/date, missing values.\n"
    "Train : choix du modèle, du feature set et lancement d'un run.\n"
    "Results : comparaison des runs et feature importances."
)
