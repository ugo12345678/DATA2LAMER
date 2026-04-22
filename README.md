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
## Sync reglementations geospatiales vers spots/zones (100% Python)

Ce repo expose maintenant un script qui remplit des tables de liaison en base Supabase,
sans API applicative.

### 1) Creer les tables en base

Executer une fois le SQL:

- `db/sql/2026-04-22_regulations_geospatial_tables.sql`

### 2) Configurer les variables d'environnement

- `SUPABASE_URL`
- `SUPABASE_KEY` (idealement service role key)

Optionnelles:

- `REG_SPOTS_TABLE` (defaut `spots`)
- `REG_ZONES_TABLE` (defaut `zones`)
- `REG_ENABLE_ZONES` (`true`/`false`, defaut `true`)
- `REG_ALLOW_SPOTS_FALLBACK_FOR_ZONE_UNION` (`true`/`false`, defaut `true`)
- `REG_SOURCE_CATALOG_FILE` (defaut `data/regulations/source_endpoints.json`)
- `REG_STATIC_LEGIFRANCE_RULES_FILE` (defaut `data/regulations/static_legifrance_rules.json`)
- `REG_GENERATED_RULES_FILE` (defaut `data/regulations/generated_rules.json`)
- `REG_GEOSPATIAL_RULES_FILE` (defaut `data/regulations/generated_rules.json`)
- `REG_ENABLE_PDF_OCR` (`true`/`false`, defaut `true`)
- `REG_OCR_LANG` (defaut `fra+eng`)
- `REG_OCR_MIN_TEXT_CHARS` (defaut `900`)
- `REG_OCR_MAX_PAGES` (defaut `8`)

### 3) Recuperer les regles depuis les sources officielles

```bash
python -m pscripts.regulations.build_regulations_feed
```

Sources interrogees par defaut :

- Legifrance (plongee et chasse sous-marine) avec fallback statique si blocage HTTP
- Ministere de la Mer (rappels operationnels chasse sous-marine + obligations declaratives)
- DIRM NAMO, MEMN, Mediterranee, Sud-Atlantique (tailles minimales, quotas, fermetures, especes protegees)
- data.gouv en source complementaire (priorite 3, non juridique)

Notes:
- Les PDF avec peu de texte extractible passent par une OCR (si dependances presentes).
- Chaque regle est enrichie avec une zone geospatiale explicite (facade/secteur) quand detectable, sinon `APP_ZONES_UNION`.

### 4) Lancer la synchro vers la base

```bash
python -m pscripts.sync_regulation_geospatial_links
```

### Tests unitaires geospatiaux

```bash
python -m unittest tests/test_sync_regulation_geospatial_links.py tests/test_build_regulations_feed.py -v
```

### Tables alimentees

- `reg_documents_sources`
- `reg_rules`
- `reg_rule_zones`
- `reg_spot_assignments`
- `reg_zone_assignments`
- `reg_assignment_runs`

Le workflow GitHub Actions associe est:

- `.github/workflows/sync_regulation_geospatial_links.yml`
