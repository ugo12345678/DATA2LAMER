from __future__ import annotations

import pandas as pd
import streamlit as st

from services.data_loader import get_dataset_source_label, load_dataset


st.title("🧾 Dataset")

SOURCE_MODE = "auto"


def filter_dataset(
    df: pd.DataFrame,
    selected_spots: list[str] | None = None,
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    out = df.copy()

    if selected_spots and "spot_id" in out.columns:
        out = out[out["spot_id"].astype(str).isin(selected_spots)]

    if "date" in out.columns:
        dt = pd.to_datetime(out["date"], errors="coerce")

        if start_date is not None:
            out = out[dt >= pd.to_datetime(start_date)]

        if end_date is not None:
            out = out[dt <= pd.to_datetime(end_date)]

    return out.reset_index(drop=True)


def summarize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    n = len(df)

    for col in df.columns:
        missing = int(df[col].isna().sum())
        rows.append(
            {
                "column": col,
                "missing_count": missing,
                "missing_pct": round((missing / n * 100), 2) if n > 0 else 0.0,
                "dtype": str(df[col].dtype),
            }
        )

    return pd.DataFrame(rows).sort_values(
        ["missing_count", "column"], ascending=[False, True]
    ).reset_index(drop=True)


try:
    df = load_dataset(source=SOURCE_MODE)
    source_label = get_dataset_source_label(source=SOURCE_MODE)
except Exception as exc:
    st.error(f"Chargement impossible : {exc}")
    st.stop()

st.caption(f"Source dataset : {source_label}")

spot_options = (
    sorted(df["spot_id"].astype(str).dropna().unique().tolist())
    if "spot_id" in df.columns
    else []
)
selected_spots = st.multiselect("Spots", options=spot_options, default=[])

start_date = None
end_date = None
if "date" in df.columns:
    dt = pd.to_datetime(df["date"], errors="coerce")
    if not dt.isna().all():
        date_min = dt.min().date()
        date_max = dt.max().date()
        date_range = st.date_input(
            "Plage de dates",
            value=(date_min, date_max),
            min_value=date_min,
            max_value=date_max,
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        elif isinstance(date_range, list) and len(date_range) == 2:
            start_date, end_date = date_range[0], date_range[1]

filtered_df = filter_dataset(
    df,
    selected_spots=selected_spots,
    start_date=start_date,
    end_date=end_date,
)

c1, c2, c3 = st.columns(3)
c1.metric("Lignes filtrées", f"{len(filtered_df):,}".replace(",", " "))
c2.metric("Colonnes", filtered_df.shape[1])
c3.metric(
    "Spots filtrés",
    filtered_df["spot_id"].nunique() if "spot_id" in filtered_df.columns else 0,
)

st.markdown("### Aperçu")
st.dataframe(filtered_df.head(200), use_container_width=True)

st.markdown("### Types de colonnes")
dtypes_df = pd.DataFrame(
    {
        "column": filtered_df.columns,
        "dtype": filtered_df.dtypes.astype(str).values,
    }
)
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