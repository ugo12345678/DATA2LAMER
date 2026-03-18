from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # Python < 3.11
    except ImportError:
        tomllib = None

from config.settings import (
    BASE_DIR,
    RAW_DIR,
    TARGET_DIR,
    SPOTS_SOURCE_FILE,
    SPOTS_VALID_FILE,
    DIRS_TO_CREATE,
    MIN_VALID_DAYS_PER_SPOT,
)
from config.pipeline import BUILD_SCRIPTS, POST_BUILD_SCRIPTS

from src.utils.io_utils import ensure_dir, read_csv
from src.utils.logging_utils import log_kv
from src.pipeline.orchestrator import (
    ensure_all_downloads,
    run_script_if_needed,
    find_latest_target_parquet,
    filter_valid_spots,
    save_filtered_outputs,
    write_run_metadata,
)

APP_EXPORT_SCRIPT = "09_export_app_dataset.py"
R2_UPLOAD_SCRIPT = "upload_to_r2.py"

DEFAULT_APP_DATASET_PATH = (
    BASE_DIR / "data" / "serving" / "dataset_visibility_app.parquet"
)
DEFAULT_R2_OBJECT_KEY = "datasets/dataset_visibility_app.parquet"


def run_python_script(
    script_path: Path,
    extra_args: list[str] | None = None,
    env: dict | None = None,
) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Script introuvable: {script_path}")

    cmd = [sys.executable, str(script_path)]
    if extra_args:
        cmd.extend(extra_args)

    print(f"\n[RUN] {' '.join(cmd)}")
    subprocess.run(
        cmd,
        check=True,
        cwd=str(BASE_DIR),
        env=env if env is not None else os.environ.copy(),
    )


def should_upload_to_r2() -> bool:
    raw = os.getenv("MAIN_UPLOAD_TO_R2", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def load_local_streamlit_secrets() -> dict:
    """
    Recherche un secrets.toml local dans :
    1. .streamlit/secrets.toml
    2. app/.streamlit/secrets.toml
    """
    candidate_paths = [
        BASE_DIR / ".streamlit" / "secrets.toml",
        BASE_DIR / "app" / ".streamlit" / "secrets.toml",
    ]

    secrets_path = next((p for p in candidate_paths if p.exists()), None)
    if secrets_path is None:
        print("[SECRETS] Aucun fichier secrets.toml local détecté.")
        return {}

    if tomllib is None:
        raise RuntimeError(
            "Aucun parseur TOML disponible pour lire secrets.toml.\n"
            "Installe tomli avec : python -m pip install tomli"
        )

    print(f"[SECRETS] Fichier détecté : {secrets_path}")

    with open(secrets_path, "rb") as f:
        return tomllib.load(f)


def get_r2_config() -> dict:
    """
    Priorité :
    1. variables d'environnement
    2. fichier secrets.toml local
    """
    secrets = load_local_streamlit_secrets()
    r2_secrets = secrets.get("R2", {}) if isinstance(secrets, dict) else {}

    return {
        "bucket": os.getenv("R2_BUCKET") or r2_secrets.get("bucket"),
        "access_key_id": os.getenv("R2_ACCESS_KEY_ID") or r2_secrets.get("access_key_id"),
        "secret_access_key": os.getenv("R2_SECRET_ACCESS_KEY") or r2_secrets.get("secret_access_key"),
        "account_id": os.getenv("R2_ACCOUNT_ID") or r2_secrets.get("account_id"),
        "endpoint_url": os.getenv("R2_ENDPOINT_URL") or r2_secrets.get("endpoint_url"),
        "region": os.getenv("R2_REGION") or r2_secrets.get("region", "auto"),
        "dataset_key": os.getenv("R2_DATASET_KEY")
        or r2_secrets.get("dataset_key", DEFAULT_R2_OBJECT_KEY),
    }


def validate_r2_config() -> dict:
    cfg = get_r2_config()

    missing = [
        key
        for key in ["bucket", "access_key_id", "secret_access_key"]
        if not cfg.get(key)
    ]
    if missing:
        raise RuntimeError(
            "Configuration R2 manquante pour l'upload automatique : "
            + ", ".join(missing)
            + "\n"
            + "Renseigne soit les variables d'environnement, "
            + "soit un fichier .streamlit/secrets.toml ou app/.streamlit/secrets.toml"
        )

    if not cfg.get("endpoint_url") and not cfg.get("account_id"):
        raise RuntimeError(
            "Configuration R2 incomplète : il faut définir soit 'endpoint_url', "
            "soit 'account_id'."
        )

    return cfg


def upload_app_dataset_to_r2() -> None:
    if not should_upload_to_r2():
        print("\n[R2 UPLOAD] Upload désactivé via MAIN_UPLOAD_TO_R2.")
        return

    cfg = validate_r2_config()

    dataset_path = Path(os.getenv("APP_DATASET_OUTPUT", str(DEFAULT_APP_DATASET_PATH)))
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Dataset app introuvable pour upload R2 : {dataset_path}"
        )

    object_key = cfg.get("dataset_key") or DEFAULT_R2_OBJECT_KEY
    upload_script_path = BASE_DIR / "scripts" / R2_UPLOAD_SCRIPT

    env = os.environ.copy()
    env["R2_BUCKET"] = cfg["bucket"]
    env["R2_ACCESS_KEY_ID"] = cfg["access_key_id"]
    env["R2_SECRET_ACCESS_KEY"] = cfg["secret_access_key"]
    env["R2_REGION"] = cfg.get("region", "auto")

    if cfg.get("endpoint_url"):
        env["R2_ENDPOINT_URL"] = cfg["endpoint_url"]
    if cfg.get("account_id"):
        env["R2_ACCOUNT_ID"] = cfg["account_id"]

    print("\n[R2 UPLOAD] Début upload vers Cloudflare R2")
    print(f"[R2 UPLOAD] Fichier local : {dataset_path}")
    print(f"[R2 UPLOAD] Objet distant : {object_key}")

    run_python_script(
        upload_script_path,
        extra_args=[
            "--file",
            str(dataset_path),
            "--key",
            object_key,
        ],
        env=env,
    )

    print("[R2 UPLOAD] Upload terminé avec succès.")


def main() -> None:
    for directory in DIRS_TO_CREATE:
        ensure_dir(directory)

    ensure_dir(BASE_DIR / "data" / "serving")

    log_kv("BASE_DIR", BASE_DIR)
    log_kv("RAW_DIR", RAW_DIR)
    log_kv("TARGET_DIR", TARGET_DIR)

    downloaded = ensure_all_downloads()

    run_script_if_needed(
        "01_build_target_zsd_pipeline.py",
        extra_env={"SPOTS_FILE_OVERRIDE": str(SPOTS_SOURCE_FILE)},
    )

    target_parquet = find_latest_target_parquet()
    print(f"\nParquet target détecté : {target_parquet}")

    filtered_spots, filtered_target, counts = filter_valid_spots(
        target_parquet=target_parquet,
        spots_source_csv=SPOTS_SOURCE_FILE,
        min_valid_days=MIN_VALID_DAYS_PER_SPOT,
    )

    spots_source_df = read_csv(SPOTS_SOURCE_FILE, label="CSV spots source")
    print(f"\nSpots source: {spots_source_df['spot_id'].nunique()}")
    print(
        f"Spots valides (>= {MIN_VALID_DAYS_PER_SPOT} jours): "
        f"{filtered_spots['spot_id'].nunique()}"
    )

    saved_outputs = save_filtered_outputs(
        filtered_spots=filtered_spots,
        filtered_target=filtered_target,
        counts=counts,
    )

    extra_env = {"SPOTS_FILE_OVERRIDE": str(SPOTS_VALID_FILE)}

    for script_name in BUILD_SCRIPTS[1:]:
        run_script_if_needed(script_name, extra_env=extra_env)

    for script_name in POST_BUILD_SCRIPTS:
        run_script_if_needed(script_name)

    print(f"\n[APP EXPORT] Lancement de {APP_EXPORT_SCRIPT}")
    run_script_if_needed(APP_EXPORT_SCRIPT)

    upload_app_dataset_to_r2()

    write_run_metadata(downloaded=downloaded, saved_outputs=saved_outputs)

    print("\n✅ Pipeline + export app + upload R2 terminés")


if __name__ == "__main__":
    main()