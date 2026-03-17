# Prédiction de visibilité sous-marine en Bretagne

## Objectif
Pipeline de collecte, préparation et entraînement ML pour prédire la visibilité sous-marine sur des spots en Bretagne.

## Périmètre
- Période : 2024-2026
- 15 spots finaux exploitables
- Dataset ML-ready final produit
- Variables statiques intégrées :
  - bathymétrie
  - pente
  - distance à la côte

## Structure du projet
- `config/` : paramètres globaux et configuration pipeline
- `src/` : logique métier, utils, orchestration
- `scripts/` : scripts pipeline unitaires
- `data/config/` : fichiers de configuration de référence
- `main.py` : point d’entrée principal

## Pipeline
Ordre d’exécution :
1. `01_build_target_zsd_pipeline.py`
2. `02_build_phy_pipeline.py`
3. `03_build_wav_pipeline.py`
4. `04_build_bgc_pipeline.py`
5. `05_build_meteo_pipeline.py`
6. `05b_build_static_pipeline.py`
7. `06_join_features.py`
8. `07_prepare_ml_dataset.py`
9. `08_train_baseline_model.py`

## Exécution
```bash
python main.py