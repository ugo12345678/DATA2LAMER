# Prédiction de visibilité sous-marine en Bretagne

Repository Python pour construire un pipeline de données et de machine learning dédié à la prédiction de la visibilité sous-marine en Bretagne.

## Ce que fait le repository

Le projet permet de :

- construire une cible de visibilité (`zsd`)
- récupérer et préparer plusieurs familles de variables environnementales :
  - physique (`phy`)
  - vagues (`wav`)
  - biogéochimie (`bgc`)
  - météo (`meteo`)
  - variables statiques (`static`) :
    - bathymétrie
    - pente
    - distance à la côte
- joindre toutes les features
- préparer un dataset final pour le machine learning
- entraîner un modèle baseline

Le pipeline est organisé autour de scripts Python exécutables individuellement ou via `main.py`.

## Structure du projet

```text
.
├── config/
├── src/
│   ├── pipeline/
│   └── utils/
├── scripts/
├── data/
│   └── config/
│       └── spots_bretagne_mvp_50.csv
├── main.py
├── requirements.txt
├── README.md
└── .gitignore
Scripts du pipeline

Ordre d’exécution :

01_build_target_zsd_pipeline.py

02_build_phy_pipeline.py

03_build_wav_pipeline.py

04_build_bgc_pipeline.py

05_build_meteo_pipeline.py

05b_build_static_pipeline.py

06_join_features.py

07_prepare_ml_dataset.py

08_train_baseline_model.py

Préparer l’environnement de travail
1. Créer un environnement virtuel

Sous Windows PowerShell :

python -m venv .venv
.venv\Scripts\Activate.ps1
2. Mettre à jour pip
python -m pip install --upgrade pip
3. Installer les dépendances Python
pip install -r requirements.txt
Compte Copernicus Marine

Certaines données utilisées par le pipeline nécessitent un compte Copernicus Marine.

Étapes :

créer un compte sur le portail Copernicus Marine

confirmer l’email

se connecter avec ses identifiants

Installer la CLI Copernicus

Si copernicusmarine est déjà présent dans requirements.txt, rien à faire de plus.

Sinon :

pip install copernicusmarine

Vérifier l’installation :

copernicusmarine --version
Se connecter à Copernicus
copernicusmarine login
Lancer le pipeline

Depuis la racine du projet :

python main.py
Exécution script par script

Si besoin, les scripts peuvent aussi être lancés individuellement :

python scripts/01_build_target_zsd_pipeline.py
python scripts/02_build_phy_pipeline.py
python scripts/03_build_wav_pipeline.py
python scripts/04_build_bgc_pipeline.py
python scripts/05_build_meteo_pipeline.py
python scripts/05b_build_static_pipeline.py
python scripts/06_join_features.py
python scripts/07_prepare_ml_dataset.py
python scripts/08_train_baseline_model.py
Données versionnées

Le fichier suivant est volontairement conservé dans Git :

data/config/spots_bretagne_mvp_50.csv

Données non versionnées

Les dossiers de sortie générés par le pipeline ne doivent pas être versionnés :

data/raw/

data/processed/

data/models/

data/reports/

data/predictions/