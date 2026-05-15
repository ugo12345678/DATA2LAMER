# DATA2LAMER - donnees meteo, marines et observations de plongee

Repository Python pour recuperer, normaliser et consolider des donnees environnementales autour des spots de plongee.

Le projet ne produit pas de prediction de visibilite et n'utilise pas de modele ML. La phase courante consiste a construire un socle fiable :

- previsions meteo et marines horaires sur 5 a 7 jours par spot ;
- consolidation multi-sources quand plusieurs fournisseurs donnent la meme metrique ;
- conservation de la provenance des valeurs utilisees ;
- jointure future avec les sorties de plongee et la visibilite reellement observee.

## Architecture BDD

Deux bases sont prevues.

### VU2LAMER

Base applicative. Elle ne recoit qu'une table de forecast consolidee :

```text
db/vu2lamer/2026-05-14_environment_forecasts.sql
```

Table principale :

```text
public.environment_forecasts
```

Une ligne correspond a un `spot_id` du catalogue `public.spots` et une heure UTC (`valid_time`). Les colonnes contiennent les valeurs consolidees : vent, rafales, houle, vagues, temperature air/eau, pression, nuages, pluie, visibilite meteo, niveau marin, courants, salinite, chlorophylle, etc.

La colonne `provenance` conserve, par metrique, les sources et valeurs utilisees pour calculer la valeur finale.
Par defaut, cette provenance est compactee (`APP_PROVENANCE_MODE=compact`) pour limiter le poids de la table applicative. Le mode `full` garde les valeurs source detaillees dans VU2LAMER, mais DATA2LAMER reste l'endroit recommande pour l'historique brut.
La migration supprime l'ancienne table `forecast_predictions`.

### DATA2LAMER

Base technique, vide au depart. Elle stocke les tables de travail si elle est configuree :

```text
db/data2lamer/2026-05-14_environment_forecast_pipeline.sql
```

Tables :

- `environment_sources`
- `environment_sync_runs`
- `forecast_source_values`
- `spot_source_grid_points`

Si les variables `DATA2LAMER_SUPABASE_URL` et `DATA2LAMER_SUPABASE_SERVICE_KEY` ne sont pas definies, le pipeline fonctionne quand meme.
Pour eviter de saturer Postgres avec les valeurs horaires brutes, l'archive recommandee est Cloudflare R2 : un fichier `jsonl.gz` par source et par run.

## Sources gratuites

Sources activees sans abonnement payant :

- Open-Meteo Weather Best Match : meteo horaire sans cle API.
- Open-Meteo Météo-France : AROME/ARPEGE via Open-Meteo, sans cle API.
- Open-Meteo DWD ICON : modele allemand ICON via Open-Meteo, sans cle API.
- Open-Meteo NOAA GFS : modele global GFS via Open-Meteo, sans cle API.
- MET Norway Locationforecast : source meteo independante sans cle API, avec `User-Agent` obligatoire.
- Open-Meteo Marine Best Match : vagues, houle, SST, courants et niveau marin sans cle API.
- Open-Meteo Marine par modele : Météo-France Wave/Currents/SST, DWD EWAM/GWAM, NOAA GFS Wave.
- Copernicus Marine / CMEMS : optionnel, gratuit avec identifiants `CMEMS_USERNAME` et `CMEMS_PASSWORD`.

Meteo-France Marine et SHOM ne sont pas integres pour l'instant afin d'eviter les dependances a cle payante ou a conditions d'acces plus lourdes.

## Synchronisation

Le job GitHub Actions `Environment Forecast Sync` lance :

```bash
python -m pscripts.environment.sync_environment_forecasts
```

Variables utiles :

```text
VU2LAMER_SUPABASE_URL
VU2LAMER_SUPABASE_SERVICE_KEY
DATA2LAMER_SUPABASE_URL
DATA2LAMER_SUPABASE_SERVICE_KEY
R2_ENDPOINT_URL
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
R2_BUCKET
R2_SOURCE_VALUES_PREFIX=environment/source_values
FORECAST_DAYS=3
FORECAST_TARGET_TIMEZONE=Europe/Paris
OPEN_METEO_BATCH_SIZE=50
OPEN_METEO_MIN_REQUEST_INTERVAL_SEC=3.0
OPEN_METEO_MINUTELY_RATE_LIMIT_SLEEP_SEC=65
OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC=3600
OPEN_METEO_MAX_RETRIES=3
APP_PROVENANCE_MODE=compact
DATA2LAMER_STORE_SOURCE_VALUES=false
FORECAST_THREAD_WORKERS=2
FORECAST_SOURCES=open_meteo_weather,open_meteo_dwd_icon,open_meteo_marine,open_meteo_marine_meteofrance_wave,metno_locationforecast
ENABLE_CMEMS=false
ENABLE_METNO=true
CMEMS_USERNAME
CMEMS_PASSWORD
METNO_USER_AGENT
```

Par defaut, le workflow programme reste volontairement limite aux sources rapides pour eviter les timeouts GitHub Actions et les limites horaires Open-Meteo. Pour un run complet manuel, definir `FORECAST_SOURCES` avec les codes voulus et passer `ENABLE_CMEMS=true` si les datasets Copernicus doivent etre interroges.

## Alertes

Les alertes lisent maintenant `environment_forecasts` et evaluent les conditions heure par heure sur la date cible.

```bash
python -m pscripts.check_alerts
```

## Recuperation des reglementations

Le module `pscripts.regulations.build_regulations_feed` genere un fichier de regles depuis les sources officielles referencees dans `data/regulations/source_endpoints.json`.

```bash
python -m pscripts.regulations.discover_regulation_sources
python -m pscripts.regulations.build_regulations_feed
python -m pscripts.refresh_regulations_database
```

Le refresh ne materialise plus les associations reglementation -> spots/zones applicatifs. Les tables historiques volumineuses `reg_spot_assignments` et `reg_zone_assignments` peuvent etre supprimees avec `db/sql/2026-05-14_drop_regulation_app_assignments.sql`.
