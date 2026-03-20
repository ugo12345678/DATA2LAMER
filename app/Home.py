from __future__ import annotations

import pandas as pd
import streamlit as st

from services.data_loader import get_dataset_source_label, load_dataset
from services.feature_sets import get_feature_sets


st.set_page_config(
    page_title="Bretagne Visibility ML",
    page_icon="🌊",
    layout="wide",
)

st.title("🌊 Bretagne Visibility ML")
st.caption("V1 Streamlit pour explorer le dataset et lancer des entraînements depuis l'interface.")

if "runs" not in st.session_state:
    st.session_state["runs"] = []

SOURCE_MODE = "auto"


def format_date(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "-"
    return str(ts.date())


def build_dataset_path_label(source_label: str) -> str:
    if source_label == "R2":
        try:
            r2 = st.secrets["R2"]
            bucket = r2.get("bucket", "?")
            key = r2.get("dataset_key", "datasets/dataset_visibility_app.parquet")
            return f"r2://{bucket}/{key}"
        except Exception:
            return "R2"
    return "Local fallback"


try:
    df = load_dataset(source=SOURCE_MODE)
    source_label = get_dataset_source_label(source=SOURCE_MODE)
    dataset_path_label = build_dataset_path_label(source_label)
    feature_sets = get_feature_sets(df)
except Exception as exc:
    st.error(f"Impossible de charger le dataset : {exc}")
    st.stop()

n_rows, n_cols = df.shape

spot_col = "spot_id" if "spot_id" in df.columns else ("spot_name" if "spot_name" in df.columns else None)
n_spots = int(df[spot_col].nunique()) if spot_col else None

date_min = None
date_max = None
if "date" in df.columns:
    dt = pd.to_datetime(df["date"], errors="coerce")
    if not dt.isna().all():
        date_min = dt.min()
        date_max = dt.max()

numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
categorical_cols = df.select_dtypes(include=["object", "category", "bool"]).columns.tolist()

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Lignes", f"{n_rows:,}".replace(",", " "))
col2.metric("Colonnes", n_cols)
col3.metric("Spots", n_spots if n_spots is not None else "-")
col4.metric("Runs comparés", len(st.session_state["runs"]))
col5.metric("Source", source_label)

st.markdown("### Dataset chargé")
st.code(dataset_path_label)

left, right = st.columns([1.2, 1])

with left:
    st.markdown("### Périmètre")
    st.write(
        {
            "date_min": format_date(date_min),
            "date_max": format_date(date_max),
            "numeric_cols": len(numeric_cols),
            "categorical_cols": len(categorical_cols),
        }
    )

with right:
    st.markdown("### Feature sets disponibles")
    for fs in feature_sets.values():
        st.markdown(f"**{fs.label}** — {len(fs.columns)} colonnes")
        st.caption(fs.description)

st.markdown("### Aperçu du dataset")
st.dataframe(df.head(20), use_container_width=True)

st.markdown("### Navigation")
st.info(
    "Dataset : inspection des colonnes, filtre par spot/date, missing values.\n"
    "Train : choix du modèle, du feature set et lancement d'un run.\n"
    "Results : comparaison des runs et feature importances."
)