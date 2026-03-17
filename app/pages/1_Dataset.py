from __future__ import annotations

import pandas as pd
import streamlit as st

from app.services.data_loader import (
    filter_dataset,
    load_dataset,
    resolve_dataset_path,
    summarize_missing_values,
)


st.title("🧾 Dataset")

try:
    df = load_dataset(resolve_dataset_path())
except Exception as exc:
    st.error(f"Chargement impossible : {exc}")
    st.stop()

spot_options = sorted(df["spot_id"].astype(str).unique().tolist()) if "spot_id" in df.columns else []
selected_spots = st.multiselect("Spots", options=spot_options, default=[])

start_date = None
end_date = None
if "date" in df.columns:
    date_min = pd.to_datetime(df["date"], errors="coerce").min().date()
    date_max = pd.to_datetime(df["date"], errors="coerce").max().date()
    start_date, end_date = st.date_input(
        "Plage de dates",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max,
    )

filtered_df = filter_dataset(df, selected_spots=selected_spots, start_date=start_date, end_date=end_date)

c1, c2, c3 = st.columns(3)
c1.metric("Lignes filtrées", f"{len(filtered_df):,}".replace(",", " "))
c2.metric("Colonnes", filtered_df.shape[1])
c3.metric("Spots filtrés", filtered_df["spot_id"].nunique() if "spot_id" in filtered_df.columns else 0)

st.markdown("### Aperçu")
st.dataframe(filtered_df.head(200), use_container_width=True)

st.markdown("### Types de colonnes")
dtypes_df = pd.DataFrame({"column": filtered_df.columns, "dtype": filtered_df.dtypes.astype(str).values})
st.dataframe(dtypes_df, use_container_width=True, height=320)

st.markdown("### Valeurs manquantes")
missing_df = summarize_missing_values(filtered_df)
st.dataframe(missing_df.head(100), use_container_width=True, height=420)

csv_bytes = filtered_df.head(5000).to_csv(index=False).encode("utf-8")
st.download_button(
    "Télécharger un extrait CSV (max 5000 lignes)",
    data=csv_bytes,
    file_name="dataset_preview.csv",
    mime="text/csv",
)
