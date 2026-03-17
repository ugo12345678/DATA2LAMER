from __future__ import annotations

import pandas as pd


def build_column_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    n = len(df)

    for col in df.columns:
        s = df[col]
        non_null = int(s.notna().sum())
        null = int(s.isna().sum())
        null_pct = round((null / n) * 100, 2) if n else 0.0
        n_unique = int(s.nunique(dropna=True))

        rows.append(
            {
                "column": col,
                "dtype": str(s.dtype),
                "non_null": non_null,
                "null": null,
                "null_pct": null_pct,
                "n_unique": n_unique,
            }
        )

    out = pd.DataFrame(rows).sort_values(["null_pct", "column"], ascending=[False, True]).reset_index(drop=True)
    return out


def build_numeric_summary(df: pd.DataFrame) -> pd.DataFrame:
    num_cols = df.select_dtypes(include=["number", "bool"]).columns.tolist()
    if not num_cols:
        return pd.DataFrame(columns=["column", "min", "max", "mean", "std", "median"])

    rows = []
    for col in num_cols:
        s = pd.to_numeric(df[col], errors="coerce")
        rows.append(
            {
                "column": col,
                "min": s.min(),
                "max": s.max(),
                "mean": s.mean(),
                "std": s.std(),
                "median": s.median(),
            }
        )
    return pd.DataFrame(rows).sort_values("column").reset_index(drop=True)


def build_spot_counts(df: pd.DataFrame, spot_col: str = "spot_id") -> pd.DataFrame:
    if spot_col not in df.columns:
        return pd.DataFrame(columns=[spot_col, "n_rows"])
    out = (
        df.groupby(spot_col, dropna=False)
        .size()
        .reset_index(name="n_rows")
        .sort_values("n_rows", ascending=True)
        .reset_index(drop=True)
    )
    return out


def build_spot_date_coverage(
    df: pd.DataFrame,
    spot_col: str = "spot_id",
    date_col: str = "date",
) -> pd.DataFrame:
    if spot_col not in df.columns or date_col not in df.columns:
        return pd.DataFrame(columns=[spot_col, "date_min", "date_max", "n_days"])

    tmp = df[[spot_col, date_col]].copy()
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")

    out = (
        tmp.groupby(spot_col, dropna=False)[date_col]
        .agg(date_min="min", date_max="max", n_days="count")
        .reset_index()
        .sort_values("n_days", ascending=True)
        .reset_index(drop=True)
    )
    return out