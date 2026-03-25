"""
Évalue les alertes actives contre les prédictions forecast en base,
puis envoie un email aux utilisateurs concernés via Resend.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests as http_requests

from pscripts.supabase_client import get_supabase

RESEND_API_KEY = os.environ["RESEND_API_KEY"]
RESEND_FROM_EMAIL = os.environ.get("RESEND_FROM_EMAIL", "onboarding@resend.dev")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")
MIN_DATA_COMPLETENESS = float(os.environ.get("MIN_DATA_COMPLETENESS", "0.5"))


# ─── Opérateurs de comparaison ───────────────────────────────────────────────

OPERATORS = {
    ">": lambda val, thr: val > thr,
    ">=": lambda val, thr: val >= thr,
    "<": lambda val, thr: val < thr,
    "<=": lambda val, thr: val <= thr,
    "=": lambda val, thr: val == thr,
    "==": lambda val, thr: val == thr,
    "!=": lambda val, thr: val != thr,
}


# ─── Mapping condition_type.label → champ forecast ──────────────────────────

CONDITION_FIELD_MAP = {
    # Visibilité (prédiction principale)
    "visibilité": "pred_visibility",
    "visibility": "pred_visibility",
    # Température de l'eau (SST)
    "température de l'eau": "sst",
    "water_temperature": "sst",
    # Température de l'air
    "température de l'air": "temperature_2m",
    "air_temperature": "temperature_2m",
    # Hauteur des vagues
    "hauteur des vagues": "wave_height",
    "wave_height": "wave_height",
    # Vitesse du vent
    "vitesse du vent": "wind_speed",
    "wind_speed": "wind_speed",
}


# ─── Chargement des données ──────────────────────────────────────────────────


def load_active_alerts() -> list[dict]:
    """Charge les alertes actives avec conditions, spots et profil utilisateur."""
    client = get_supabase()

    alerts_resp = (
        client.table("alerts")
        .select(
            "id, user_id, name, description, notification_type, forecast_day, "
            "alert_conditions(id, condition_type_id, operator, threshold_value), "
            "alert_spots(spot_id)"
        )
        .eq("is_active", True)
        .execute()
    )
    return alerts_resp.data or []


def load_condition_types() -> dict[str, dict]:
    """Retourne un dict {id: {label, unit, ...}} pour les condition_types."""
    client = get_supabase()
    resp = client.table("condition_types").select("id, label, description, unit").execute()
    return {row["id"]: row for row in (resp.data or [])}


def load_user_emails(user_ids: list[str]) -> dict[str, str]:
    """Charge les emails depuis auth.users via une RPC ou la table profiles."""
    if not user_ids:
        return {}

    client = get_supabase()

    # Essayer via une RPC Supabase pour récupérer les emails
    # (les emails sont dans auth.users, pas directement accessible en client-side,
    #  donc on utilise une fonction RPC côté Supabase)
    try:
        resp = client.rpc("get_user_emails", {"user_ids": user_ids}).execute()
        if resp.data:
            return {row["id"]: row["email"] for row in resp.data}
    except Exception:
        pass

    # Fallback : utiliser la vue profiles si elle contient l'email
    # ou le service_role key qui a accès à auth.users
    try:
        resp = (
            client.table("profiles")
            .select("id, email")
            .in_("id", user_ids)
            .execute()
        )
        if resp.data:
            return {row["id"]: row["email"] for row in resp.data if row.get("email")}
    except Exception:
        pass

    return {}


def load_forecasts_for_spots(spot_ids: list, target_date: str) -> list[dict]:
    """Charge les prédictions forecast pour des spots à une date cible."""
    client = get_supabase()
    resp = (
        client.table("forecast_predictions")
        .select("spot_id, target_time, pred_visibility, data_completeness, features_json")
        .in_("spot_id", spot_ids)
        .eq("model_version", MODEL_VERSION)
        .gte("data_completeness", MIN_DATA_COMPLETENESS)
        .gte("target_time", f"{target_date}T00:00:00+00:00")
        .lt("target_time", f"{target_date}T23:59:59+00:00")
        .execute()
    )
    return resp.data or []


# ─── Évaluation des conditions ───────────────────────────────────────────────


def get_forecast_value(prediction: dict, field: str) -> float | None:
    """Extrait une valeur du forecast (soit colonne directe, soit dans features_json)."""
    # Champs directs sur la row
    if field in prediction and prediction[field] is not None:
        try:
            return float(prediction[field])
        except (ValueError, TypeError):
            return None

    # Sinon chercher dans features_json
    features = prediction.get("features_json")
    if isinstance(features, dict) and field in features:
        val = features[field]
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

    return None


def evaluate_condition(
    prediction: dict,
    condition: dict,
    condition_types: dict[str, dict],
) -> bool | None:
    """
    Évalue une condition contre un forecast.
    Retourne True (condition remplie), False, ou None (donnée manquante).
    """
    ctype = condition_types.get(condition["condition_type_id"])
    if not ctype:
        print(f"  [WARN] condition_type_id inconnu: {condition['condition_type_id']}")
        return None

    label = ctype["label"].lower().strip()
    field = CONDITION_FIELD_MAP.get(label)
    if not field:
        print(f"  [WARN] label condition non mappé: '{label}'")
        return None

    operator_fn = OPERATORS.get(condition["operator"])
    if not operator_fn:
        print(f"  [WARN] opérateur inconnu: '{condition['operator']}'")
        return None

    value = get_forecast_value(prediction, field)
    if value is None:
        return None

    threshold = float(condition["threshold_value"])
    return operator_fn(value, threshold)


def evaluate_alert(
    alert: dict,
    condition_types: dict[str, dict],
    forecasts_by_spot: dict[str, dict],
) -> list[dict]:
    """
    Évalue une alerte sur tous ses spots.
    Retourne la liste des spots déclenchés avec les détails.
    """
    conditions = alert.get("alert_conditions") or []
    if not conditions:
        return []

    triggered_spots = []

    for alert_spot in (alert.get("alert_spots") or []):
        spot_id = alert_spot["spot_id"]
        prediction = forecasts_by_spot.get(spot_id)
        if not prediction:
            continue

        results = []
        for cond in conditions:
            result = evaluate_condition(prediction, cond, condition_types)
            results.append(result)

        # Toutes les conditions doivent être remplies (AND logic)
        if all(r is True for r in results):
            triggered_spots.append({
                "spot_id": spot_id,
                "prediction": prediction,
            })

    return triggered_spots


# ─── Envoi d'emails ──────────────────────────────────────────────────────────


def format_alert_email(alert: dict, triggered: list[dict], condition_types: dict) -> tuple[str, str]:
    """Génère le sujet et le corps HTML de l'email."""
    subject = f"🌊 Alerte DATA2LAMER : {alert['name']}"

    spots_html = ""
    for t in triggered:
        pred = t["prediction"]
        visibility = pred.get("pred_visibility")
        completeness = pred.get("data_completeness", 0)
        vis_text = f"{visibility:.1f} m" if visibility is not None else "N/A"
        spots_html += f"""
        <tr>
            <td style="padding:8px;border:1px solid #ddd;">{t['spot_id']}</td>
            <td style="padding:8px;border:1px solid #ddd;">{vis_text}</td>
            <td style="padding:8px;border:1px solid #ddd;">{completeness:.0%}</td>
        </tr>"""

    conditions_html = ""
    for cond in (alert.get("alert_conditions") or []):
        ctype = condition_types.get(cond["condition_type_id"], {})
        label = ctype.get("label", "?")
        unit = ctype.get("unit", "")
        conditions_html += f"<li>{label} {cond['operator']} {cond['threshold_value']} {unit}</li>"

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
        <h2 style="color:#1a56db;">🌊 Alerte : {alert['name']}</h2>
        <p>{alert.get('description') or ''}</p>

        <h3>Conditions déclenchées</h3>
        <ul>{conditions_html}</ul>

        <h3>Spots concernés</h3>
        <table style="border-collapse:collapse;width:100%;">
            <tr style="background:#f0f0f0;">
                <th style="padding:8px;border:1px solid #ddd;">Spot</th>
                <th style="padding:8px;border:1px solid #ddd;">Visibilité prédite</th>
                <th style="padding:8px;border:1px solid #ddd;">Fiabilité données</th>
            </tr>
            {spots_html}
        </table>

        <p style="margin-top:20px;color:#666;font-size:12px;">
            Prévision pour J+{alert.get('forecast_day', '?')} —
            Généré le {datetime.now(timezone.utc).strftime('%d/%m/%Y à %Hh%M UTC')}
        </p>
    </div>
    """
    return subject, html


def send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Envoie un email via l'API Resend."""
    resp = http_requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": RESEND_FROM_EMAIL,
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        },
        timeout=30,
    )

    if resp.status_code in (200, 201):
        print(f"  [OK] Email envoyé à {to_email}")
        return True
    else:
        print(f"  [ERROR] Échec envoi email à {to_email}: {resp.status_code} {resp.text}")
        return False


def log_alert_trigger(alert_id: str, spot_ids: list[str], sent: bool) -> None:
    """Enregistre le déclenchement dans Supabase pour éviter les doublons."""
    client = get_supabase()
    try:
        client.table("alert_triggers").upsert(
            {
                "alert_id": alert_id,
                "triggered_at": datetime.now(timezone.utc).isoformat(),
                "spot_ids": spot_ids,
                "email_sent": sent,
            },
            on_conflict="alert_id,triggered_date",
        ).execute()
    except Exception as exc:
        print(f"  [WARN] Échec log trigger: {exc}")


# ─── Main ────────────────────────────────────────────────────────────────────


def main() -> None:
    print("=== CHECK ALERTS ===")

    alerts = load_active_alerts()
    print(f"Alertes actives: {len(alerts)}")
    if not alerts:
        print("Aucune alerte active. Fin.")
        return

    condition_types = load_condition_types()
    print(f"Types de conditions: {len(condition_types)}")

    # Collecter tous les user_ids et spot_ids nécessaires
    all_user_ids = list({a["user_id"] for a in alerts if a.get("user_id")})
    all_spot_ids = list({
        s["spot_id"]
        for a in alerts
        for s in (a.get("alert_spots") or [])
    })

    print(f"Utilisateurs concernés: {len(all_user_ids)}")
    print(f"Spots à vérifier: {len(all_spot_ids)}")

    user_emails = load_user_emails(all_user_ids)
    print(f"Emails récupérés: {len(user_emails)}")

    # Évaluer chaque alerte
    emails_sent = 0
    alerts_triggered = 0

    for alert in alerts:
        alert_name = alert.get("name", alert["id"])
        forecast_day = alert.get("forecast_day") or 0
        target_date = (
            datetime.now(timezone.utc) + timedelta(days=forecast_day)
        ).strftime("%Y-%m-%d")

        spot_ids = [s["spot_id"] for s in (alert.get("alert_spots") or [])]
        if not spot_ids:
            print(f"[{alert_name}] Aucun spot associé, skip.")
            continue

        print(f"\n[{alert_name}] forecast_day=J+{forecast_day} ({target_date}), "
              f"{len(spot_ids)} spot(s), {len(alert.get('alert_conditions') or [])} condition(s)")

        forecasts = load_forecasts_for_spots(spot_ids, target_date)
        if not forecasts:
            print(f"  Aucune prédiction trouvée pour {target_date}")
            continue

        # Dédupliquer : garder la meilleure completeness par spot
        forecasts_by_spot: dict[str, dict] = {}
        for f in forecasts:
            sid = f["spot_id"]
            if sid not in forecasts_by_spot or (
                (f.get("data_completeness") or 0) > (forecasts_by_spot[sid].get("data_completeness") or 0)
            ):
                forecasts_by_spot[sid] = f

        print(f"  Prédictions disponibles: {len(forecasts_by_spot)} spot(s)")

        triggered = evaluate_alert(alert, condition_types, forecasts_by_spot)
        if not triggered:
            print(f"  Aucun spot ne déclenche l'alerte.")
            continue

        alerts_triggered += 1
        triggered_spot_ids = [t["spot_id"] for t in triggered]
        print(f"  ALERTE DÉCLENCHÉE sur {len(triggered)} spot(s): {triggered_spot_ids}")

        # Envoyer l'email
        user_email = user_emails.get(alert["user_id"])
        if not user_email:
            print(f"  [WARN] Pas d'email trouvé pour user {alert['user_id']}")
            log_alert_trigger(alert["id"], triggered_spot_ids, sent=False)
            continue

        subject, html = format_alert_email(alert, triggered, condition_types)
        sent = send_email(user_email, subject, html)
        if sent:
            emails_sent += 1
        log_alert_trigger(alert["id"], triggered_spot_ids, sent=sent)

    print(f"\n=== RÉSUMÉ: {alerts_triggered} alerte(s) déclenchée(s), {emails_sent} email(s) envoyé(s) ===")

    # Générer le rapport JSON pour l'artefact GitHub Actions
    report = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "alerts_active": len(alerts),
        "alerts_triggered": alerts_triggered,
        "emails_sent": emails_sent,
        "users_concerned": len(all_user_ids),
        "spots_checked": len(all_spot_ids),
    }

    report_dir = Path("artifacts")
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / "alert_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"Rapport sauvegardé: {report_path}")


if __name__ == "__main__":
    main()
