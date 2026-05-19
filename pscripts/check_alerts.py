"""
Evaluate active alerts against consolidated hourly weather/marine forecasts.

Observed visibility is still collected from dive outings; this script only evaluates
forecastable environmental conditions.
"""
from __future__ import annotations

import json
import os
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests as http_requests

from pscripts.supabase_client import get_vu2lamer_supabase


RESEND_API_KEY = os.environ["RESEND_API_KEY"]
RESEND_FROM_EMAIL = os.environ.get("RESEND_FROM_EMAIL", "onboarding@resend.dev")

FORECAST_TABLE = os.environ.get("VU2LAMER_FORECAST_TABLE", "environment_forecasts")
FORECAST_TARGET_TIMEZONE = os.environ.get("FORECAST_TARGET_TIMEZONE", "Europe/Paris")


OPERATORS = {
    ">": lambda val, thr: val > thr,
    ">=": lambda val, thr: val >= thr,
    "<": lambda val, thr: val < thr,
    "<=": lambda val, thr: val <= thr,
    "=": lambda val, thr: val == thr,
    "==": lambda val, thr: val == thr,
    "!=": lambda val, thr: val != thr,
}

CONDITION_FIELD_MAP = {
    "temperature de l'eau": "water_temperature_c",
    "water_temperature": "water_temperature_c",
    "temperature de l'air": "air_temperature_c",
    "air_temperature": "air_temperature_c",
    "humidite": "relative_humidity_pct",
    "relative_humidity": "relative_humidity_pct",
    "point de rosee": "dew_point_c",
    "dew_point": "dew_point_c",
    "hauteur des vagues": "wave_height_m",
    "wave_height": "wave_height_m",
    "periode des vagues": "wave_period_s",
    "wave_period": "wave_period_s",
    "houle": "swell_wave_height_m",
    "swell_wave_height": "swell_wave_height_m",
    "periode de houle": "swell_wave_period_s",
    "swell_wave_period": "swell_wave_period_s",
    "vitesse du vent": "wind_speed_ms",
    "wind_speed": "wind_speed_ms",
    "rafales de vent": "wind_gusts_ms",
    "wind_gusts": "wind_gusts_ms",
    "precipitations": "precipitation_mm",
    "precipitation": "precipitation_mm",
    "couverture nuageuse": "cloud_cover_pct",
    "cloud_cover": "cloud_cover_pct",
    "nuages bas": "cloud_cover_low_pct",
    "cloud_cover_low": "cloud_cover_low_pct",
    "nuages moyens": "cloud_cover_mid_pct",
    "cloud_cover_mid": "cloud_cover_mid_pct",
    "nuages hauts": "cloud_cover_high_pct",
    "cloud_cover_high": "cloud_cover_high_pct",
    "pression": "pressure_msl_hpa",
    "pressure": "pressure_msl_hpa",
    "visibilite meteo": "weather_visibility_m",
    "weather_visibility": "weather_visibility_m",
    "maree": "sea_level_height_m",
    "sea_level": "sea_level_height_m",
    "coefficient de maree": "tide_coefficient",
    "coef de maree": "tide_coefficient",
    "tide_coefficient": "tide_coefficient",
    "courant": "current_speed_ms",
    "current_speed": "current_speed_ms",
    "salinite": "salinity_psu",
    "salinity": "salinity_psu",
    "chlorophylle": "chlorophyll_mg_m3",
    "chlorophyll": "chlorophyll_mg_m3",
    "carbone phytoplancton": "phytoplankton_carbon_mmol_m3",
    "phytoplankton_carbon": "phytoplankton_carbon_mmol_m3",
    "production primaire": "net_primary_production_mg_m3_day",
    "net_primary_production": "net_primary_production_mg_m3_day",
    "profondeur euphotique": "euphotic_depth_m",
    "euphotic_depth": "euphotic_depth_m",
    "risque bloom algal": "algal_bloom_risk",
    "bloom algal": "algal_bloom_risk",
    "efflorescence planctonique": "algal_bloom_risk",
    "algal_bloom_risk": "algal_bloom_risk",
}

FORECAST_SELECT_COLUMNS = [
    "spot_id",
    "target_date",
    "valid_time",
    "forecast_run_at",
    "sources",
    "wind_speed_ms",
    "wind_gusts_ms",
    "wind_direction_deg",
    "air_temperature_c",
    "relative_humidity_pct",
    "dew_point_c",
    "pressure_msl_hpa",
    "surface_pressure_hpa",
    "cloud_cover_pct",
    "cloud_cover_low_pct",
    "cloud_cover_mid_pct",
    "cloud_cover_high_pct",
    "precipitation_mm",
    "weather_visibility_m",
    "wave_height_m",
    "wave_period_s",
    "wave_direction_deg",
    "swell_wave_height_m",
    "swell_wave_period_s",
    "swell_wave_direction_deg",
    "water_temperature_c",
    "sea_level_height_m",
    "tide_coefficient",
    "current_speed_ms",
    "current_direction_deg",
    "salinity_psu",
    "chlorophyll_mg_m3",
    "phytoplankton_carbon_mmol_m3",
    "net_primary_production_mg_m3_day",
    "euphotic_depth_m",
    "algal_bloom_risk",
    "light_attenuation_m1",
]


def normalize_label(label: str) -> str:
    normalized = unicodedata.normalize("NFKD", label)
    without_accents = "".join(c for c in normalized if not unicodedata.combining(c))
    return without_accents.lower().strip()


def client():
    return get_vu2lamer_supabase()


def load_active_alerts() -> list[dict]:
    resp = (
        client()
        .table("alerts")
        .select(
            "id, user_id, name, description, notification_type, forecast_day, "
            "alert_conditions(id, condition_type_id, operator, threshold_value), "
            "alert_spots(spot_id)"
        )
        .eq("is_active", True)
        .execute()
    )
    return resp.data or []


def load_condition_types() -> dict[str, dict]:
    resp = client().table("condition_types").select("id, label, description, unit").execute()
    return {row["id"]: row for row in (resp.data or [])}


def load_user_emails(user_ids: list[str]) -> dict[str, str]:
    if not user_ids:
        return {}

    db = client()
    try:
        resp = db.rpc("get_user_emails", {"user_ids": user_ids}).execute()
        if resp.data:
            return {row["id"]: row["email"] for row in resp.data}
    except Exception:
        pass

    try:
        resp = db.table("profiles").select("id, email").in_("id", user_ids).execute()
        if resp.data:
            return {row["id"]: row["email"] for row in resp.data if row.get("email")}
    except Exception:
        pass

    return {}


def load_forecasts_for_spots(spot_ids: list, target_date: str) -> list[dict]:
    resp = (
        client()
        .table(FORECAST_TABLE)
        .select(",".join(FORECAST_SELECT_COLUMNS))
        .in_("spot_id", spot_ids)
        .eq("target_date", target_date)
        .execute()
    )
    return resp.data or []


def group_forecasts_by_spot(forecasts: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for forecast in forecasts:
        grouped.setdefault(forecast["spot_id"], []).append(forecast)
    for rows in grouped.values():
        rows.sort(key=lambda item: item.get("valid_time") or "")
    return grouped


def get_forecast_value(forecast: dict, field: str) -> float | None:
    if forecast.get(field) is None:
        return None
    try:
        return float(forecast[field])
    except (ValueError, TypeError):
        return None


def evaluate_condition(forecast: dict, condition: dict, condition_types: dict[str, dict]) -> bool | None:
    ctype = condition_types.get(condition["condition_type_id"])
    if not ctype:
        print(f"  [WARN] unknown condition_type_id: {condition['condition_type_id']}")
        return None

    label = normalize_label(ctype["label"])
    field = CONDITION_FIELD_MAP.get(label)
    if not field:
        print(f"  [WARN] condition not mapped to hourly forecasts: '{label}'")
        return None

    operator_fn = OPERATORS.get(condition["operator"])
    if not operator_fn:
        print(f"  [WARN] unknown operator: '{condition['operator']}'")
        return None

    value = get_forecast_value(forecast, field)
    if value is None:
        return None

    return operator_fn(value, float(condition["threshold_value"]))


def evaluate_alert(
    alert: dict,
    condition_types: dict[str, dict],
    forecasts_by_spot: dict[str, list[dict]],
) -> list[dict]:
    conditions = alert.get("alert_conditions") or []
    if not conditions:
        return []

    triggered_spots = []
    for alert_spot in alert.get("alert_spots") or []:
        spot_id = alert_spot["spot_id"]
        forecasts = forecasts_by_spot.get(spot_id, [])

        for forecast in forecasts:
            results = [evaluate_condition(forecast, cond, condition_types) for cond in conditions]
            if all(result is True for result in results):
                triggered_spots.append({"spot_id": spot_id, "forecast": forecast})
                break

    return triggered_spots


def format_number(value, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.1f}{suffix}"
    except (TypeError, ValueError):
        return "N/A"


def format_alert_email(alert: dict, triggered: list[dict], condition_types: dict) -> tuple[str, str]:
    subject = f"Alerte DATA2LAMER : {alert['name']}"

    spots_html = ""
    for item in triggered:
        forecast = item["forecast"]
        sources = ", ".join(forecast.get("sources") or [])
        spots_html += f"""
        <tr>
            <td style="padding:8px;border:1px solid #ddd;">{item['spot_id']}</td>
            <td style="padding:8px;border:1px solid #ddd;">{forecast.get('valid_time', 'N/A')}</td>
            <td style="padding:8px;border:1px solid #ddd;">{format_number(forecast.get('air_temperature_c'), ' degC')}</td>
            <td style="padding:8px;border:1px solid #ddd;">{format_number(forecast.get('wave_height_m'), ' m')}</td>
            <td style="padding:8px;border:1px solid #ddd;">{format_number(forecast.get('wind_speed_ms'), ' m/s')}</td>
            <td style="padding:8px;border:1px solid #ddd;">{sources}</td>
        </tr>"""

    conditions_html = ""
    for cond in alert.get("alert_conditions") or []:
        ctype = condition_types.get(cond["condition_type_id"], {})
        label = ctype.get("label", "?")
        unit = ctype.get("unit", "")
        conditions_html += f"<li>{label} {cond['operator']} {cond['threshold_value']} {unit}</li>"

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;">
        <h2 style="color:#1a56db;">Alerte : {alert['name']}</h2>
        <p>{alert.get('description') or ''}</p>
        <h3>Conditions declenchees</h3>
        <ul>{conditions_html}</ul>
        <h3>Spots concernes</h3>
        <table style="border-collapse:collapse;width:100%;">
            <tr style="background:#f0f0f0;">
                <th style="padding:8px;border:1px solid #ddd;">Spot</th>
                <th style="padding:8px;border:1px solid #ddd;">Heure UTC</th>
                <th style="padding:8px;border:1px solid #ddd;">Air</th>
                <th style="padding:8px;border:1px solid #ddd;">Vagues</th>
                <th style="padding:8px;border:1px solid #ddd;">Vent</th>
                <th style="padding:8px;border:1px solid #ddd;">Sources</th>
            </tr>
            {spots_html}
        </table>
        <p style="margin-top:20px;color:#666;font-size:12px;">
            Prevision horaire pour J+{alert.get('forecast_day', '?')} -
            Genere le {datetime.now(timezone.utc).strftime('%d/%m/%Y a %Hh%M UTC')}
        </p>
    </div>
    """
    return subject, html


def send_email(to_email: str, subject: str, html_body: str) -> bool:
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
        print(f"  [OK] Email sent to {to_email}")
        return True

    print(f"  [ERROR] Email failed for {to_email}: {resp.status_code} {resp.text}")
    return False


def log_alert_notification(alert_id: str, notification_type: str, sent: bool) -> None:
    try:
        client().table("alert_notifications").insert(
            {
                "alert_id": alert_id,
                "notification_type": notification_type,
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "status": "sent" if sent else "failed",
            }
        ).execute()
    except Exception as exc:
        print(f"  [WARN] notification log failed: {exc}")


def already_notified_today(alert_ids: list[str]) -> set[str]:
    if not alert_ids:
        return set()

    today_start = datetime.now(timezone.utc).strftime("%Y-%m-%d") + "T00:00:00+00:00"
    try:
        resp = (
            client()
            .table("alert_notifications")
            .select("alert_id")
            .in_("alert_id", alert_ids)
            .eq("notification_type", "email")
            .eq("status", "sent")
            .gte("sent_at", today_start)
            .execute()
        )
        return {row["alert_id"] for row in (resp.data or [])}
    except Exception as exc:
        print(f"[WARN] existing notification lookup failed: {exc}")
        return set()


def main() -> None:
    print("=== CHECK HOURLY FORECAST ALERTS ===")

    alerts = load_active_alerts()
    print(f"Active alerts: {len(alerts)}")
    if not alerts:
        return

    condition_types = load_condition_types()
    all_user_ids = list({alert["user_id"] for alert in alerts if alert.get("user_id")})
    all_alert_ids = [alert["id"] for alert in alerts]
    notified_today = already_notified_today(all_alert_ids)
    user_emails = load_user_emails(all_user_ids)

    emails_sent = 0
    alerts_triggered = 0
    skipped_already_sent = 0

    for alert in alerts:
        alert_name = alert.get("name", alert["id"])
        if alert["id"] in notified_today:
            skipped_already_sent += 1
            print(f"[{alert_name}] already notified today, skipped.")
            continue

        forecast_day = alert.get("forecast_day") or 0
        target_tz = ZoneInfo(FORECAST_TARGET_TIMEZONE)
        target_date = (datetime.now(target_tz) + timedelta(days=forecast_day)).strftime("%Y-%m-%d")
        spot_ids = [item["spot_id"] for item in alert.get("alert_spots") or []]
        if not spot_ids:
            print(f"[{alert_name}] no spots, skipped.")
            continue

        forecasts = load_forecasts_for_spots(spot_ids, target_date)
        forecasts_by_spot = group_forecasts_by_spot(forecasts)
        print(f"[{alert_name}] {len(forecasts)} hourly forecasts for {target_date}")

        triggered = evaluate_alert(alert, condition_types, forecasts_by_spot)
        if not triggered:
            continue

        alerts_triggered += 1
        user_email = user_emails.get(alert["user_id"])
        if not user_email:
            log_alert_notification(alert["id"], "email", sent=False)
            continue

        subject, html = format_alert_email(alert, triggered, condition_types)
        sent = send_email(user_email, subject, html)
        if sent:
            emails_sent += 1
        log_alert_notification(alert["id"], "email", sent=sent)

    report = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "alerts_active": len(alerts),
        "alerts_triggered": alerts_triggered,
        "emails_sent": emails_sent,
        "skipped_already_sent": skipped_already_sent,
    }
    report_dir = Path("artifacts")
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / "alert_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
