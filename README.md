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

## Recuperation des reglementations

Le module `pscripts.regulations.build_regulations_feed` genere un fichier de regles depuis les sources officielles referencees dans `data/regulations/source_endpoints.json`.

```bash
python -m pscripts.regulations.build_regulations_feed
```

Sorties generees:

- `data/regulations/generated_rules.json`: regles triees, dedoublonnees et pretes pour la synchro Supabase.
- `data/regulations/quality_report.json`: rapport de coherence, doublons probables, conflits de metriques et sources a verifier.
- `data/regulations/generated_rule_candidates.json`: candidats d'extraction avec statut, confiance, payload structure et audit IA eventuel.
- `data/regulations/source_documents_manifest.json`: manifeste des documents sources, citations et chunks/preuves utilises par les regles.

Modele de donnees:

- `reg_source_documents` conserve les documents sources hashes.
- `reg_document_chunks` conserve les extraits/preuves.
- `reg_rule_candidates` conserve les propositions brutes a trier.
- `reg_rule_citations` relie chaque regle a sa preuve.
- `reg_species` et `reg_rule_species` normalisent les especes.
- `reg_rules.status` distingue `needs_review`, `published` et les futurs statuts d'archivage.

Pour synchroniser ces regles avec Supabase, creer d'abord les tables avec:

```bash
db/sql/2026-04-22_regulations_geospatial_tables.sql
db/sql/2026-04-29_regulations_audit_model_v2.sql
```

Puis lancer:

```bash
python -m pscripts.sync_regulation_geospatial_links
```

Audit IA optionnel avec OpenAI:

```powershell
$env:REG_ENABLE_AI_AUDIT="true"
$env:OPENAI_API_KEY="..."
python -m pscripts.regulations.build_regulations_feed
```

Audit IA gratuit avec OpenRouter:

```powershell
$env:REG_ENABLE_AI_AUDIT="true"
$env:REG_AI_API_KEY="..."
$env:REG_AI_BASE_URL="https://openrouter.ai/api/v1"
$env:REG_AI_MODEL="nvidia/nemotron-3-super-120b-a12b:free"
python -m pscripts.regulations.build_regulations_feed
```

Audit IA gratuit en local avec Ollama:

```powershell
ollama pull llama3.1
$env:REG_ENABLE_AI_AUDIT="true"
$env:REG_AI_BASE_URL="http://localhost:11434/v1"
$env:REG_AI_MODEL="llama3.1"
python -m pscripts.regulations.build_regulations_feed
```

Variables utiles:

- `REG_LEGIFRANCE_FETCH_LIVE`: tente de lire Legifrance en direct, defaut `false`. Le site peut renvoyer `403`; le socle statique versionne est alors le chemin fiable.
- `REG_AI_MODEL`: modele utilise pour l'audit IA, defaut `nvidia/nemotron-3-super-120b-a12b:free`.
- `REG_AI_BASE_URL`: endpoint OpenAI-compatible, defaut `https://openrouter.ai/api/v1`. Les endpoints locaux (`localhost`, `127.0.0.1`) ne demandent pas de cle API.
- `REG_AI_MAX_RULES`: nombre maximal de regles envoyees a l'audit IA, defaut `200`.
- L'audit IA renseigne `confidence_score`, `confidence_source="ai"` et `confidence_reason` sur les regles auditees. Les regles avec un score IA inferieur a `0.65` repassent en revue manuelle.
- `REG_ENABLE_PDF_OCR`: active/desactive l'OCR des PDF peu lisibles, defaut `false`.

OCR PDF optionnel:

```powershell
pip install pdf2image pytesseract
$env:REG_ENABLE_PDF_OCR="true"
python -m pscripts.regulations.build_regulations_feed
```

L'OCR necessite aussi les binaires systeme Poppler (`pdftoppm`) et Tesseract installes sur la machine.
