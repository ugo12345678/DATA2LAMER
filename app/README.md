# Streamlit app - Bretagne Visibility ML

## Structure

- `app/Home.py`
- `app/pages/1_Dataset.py`
- `app/pages/2_Train.py`
- `app/pages/3_Results.py`
- `app/services/data_loader.py`
- `app/services/feature_sets.py`
- `app/services/train.py`

## Lancement

Depuis la racine du repo :

```bash
streamlit run app/Home.py
```

## Chemin du dataset

L'app cherche par défaut, dans cet ordre :

1. `STREAMLIT_DATASET_PATH`
2. `data/processed/ml/dataset_visibility_mvp_ml_ready_2024_2026.parquet`
3. `data/processed/final/dataset_visibility_mvp_temporal_2024_2026.parquet`
4. `data/processed/final/dataset_visibility_mvp_2024_2026.parquet`

Exemple :

```bash
set STREAMLIT_DATASET_PATH=D:\Code\POC\data\processed\ml\dataset_visibility_mvp_ml_ready_2024_2026.parquet
streamlit run app/Home.py
```

## Dépendances minimales

- streamlit
- pandas
- scikit-learn
- lightgbm
- pyarrow

## Notes

- le split train/valid/test reprend un split temporel simple similaire à ton script `08_train_baseline_model.py`
- la comparaison des runs est stockée en mémoire de session Streamlit pour cette V1
- `temporal + zsd_lag_1` est déjà prévu : il s'activera automatiquement si la colonne existe dans le dataset
