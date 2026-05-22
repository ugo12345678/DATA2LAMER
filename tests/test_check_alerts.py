from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from pscripts import check_alerts


class CheckAlertsTest(unittest.TestCase):
    def setUp(self):
        self.condition_types = {
            "wind_gusts": {"id": "wind_gusts", "label": "Rafales de vent", "unit": "km/h"},
            "weather_visibility": {"id": "weather_visibility", "label": "Visibilité météo", "unit": "km"},
            "swell_height": {"id": "swell_height", "label": "Houle primaire", "unit": "m"},
            "secondary_swell_height": {"id": "secondary_swell_height", "label": "Houle secondaire", "unit": "m"},
            "condition_score": {"id": "condition_score", "label": "Indice météo marine", "unit": "/100"},
            "visibility": {"id": "visibility", "label": "Visibilité", "unit": "m"},
        }

    def test_wind_gust_threshold_is_converted_from_kmh_to_ms(self):
        forecast = {"wind_gusts_ms": 11.0}
        condition = {
            "condition_type_id": "wind_gusts",
            "operator": ">=",
            "threshold_value": 38.0,
            "unit_system": "metric",
        }

        self.assertTrue(check_alerts.evaluate_condition(forecast, condition, self.condition_types))

    def test_weather_visibility_threshold_is_converted_from_km_to_m(self):
        forecast = {"weather_visibility_m": 9000.0}
        condition = {
            "condition_type_id": "weather_visibility",
            "operator": ">=",
            "threshold_value": 8.0,
            "unit_system": "metric",
        }

        self.assertTrue(check_alerts.evaluate_condition(forecast, condition, self.condition_types))

    def test_current_condition_type_ids_map_to_environment_forecast_columns(self):
        primary = {
            "condition_type_id": "swell_height",
            "operator": ">",
            "threshold_value": 1.5,
            "unit_system": "metric",
        }
        secondary = {
            "condition_type_id": "secondary_swell_height",
            "operator": ">",
            "threshold_value": 0.5,
            "unit_system": "metric",
        }
        forecast = {
            "swell_wave_height_m": 1.8,
            "secondary_swell_wave_height_m": 0.7,
        }

        self.assertTrue(check_alerts.evaluate_condition(forecast, primary, self.condition_types))
        self.assertTrue(check_alerts.evaluate_condition(forecast, secondary, self.condition_types))

    def test_condition_score_is_computed_from_forecast_values(self):
        forecast = {
            "wave_height_m": 0.4,
            "wind_speed_ms": 2.0,
            "wind_gusts_ms": 4.0,
            "current_speed_ms": 0.1,
            "precipitation_mm": 0.0,
        }
        condition = {
            "condition_type_id": "condition_score",
            "operator": ">=",
            "threshold_value": 70.0,
            "unit_system": "metric",
        }

        self.assertTrue(check_alerts.evaluate_condition(forecast, condition, self.condition_types))

    def test_underwater_visibility_condition_is_not_forecastable(self):
        forecast = {"weather_visibility_m": 10000.0}
        condition = {
            "condition_type_id": "visibility",
            "operator": ">=",
            "threshold_value": 10.0,
            "unit_system": "metric",
        }

        with redirect_stdout(StringIO()):
            result = check_alerts.evaluate_condition(forecast, condition, self.condition_types)

        self.assertIsNone(result)

    def test_evaluate_alert_uses_alert_unit_system(self):
        alert = {
            "unit_system": "imperial",
            "alert_spots": [{"spot_id": "spot-1"}],
            "alert_conditions": [
                {
                    "condition_type_id": "wind_gusts",
                    "operator": ">=",
                    "threshold_value": 24.0,
                }
            ],
        }
        forecasts_by_spot = {"spot-1": [{"spot_id": "spot-1", "wind_gusts_ms": 11.0}]}

        triggered = check_alerts.evaluate_alert(alert, self.condition_types, forecasts_by_spot)

        self.assertEqual(len(triggered), 1)

    def test_alert_workflow_runs_after_forecast_publish(self):
        repo_root = Path(__file__).resolve().parents[1]
        content = (repo_root / ".github" / "workflows" / "check_alerts.yml").read_text(encoding="utf-8")

        self.assertIn('workflows: ["Environment Forecast Publish"]', content)
        self.assertNotIn('workflows: ["Environment Forecast Sync"]', content)

    def test_target_date_window_check_detects_unpublished_horizon(self):
        forecast_window = ("2026-05-22", "2026-05-25")

        self.assertTrue(check_alerts.target_date_in_window("2026-05-25", forecast_window))
        self.assertFalse(check_alerts.target_date_in_window("2026-05-27", forecast_window))

    def test_forecast_sync_default_keeps_short_horizon(self):
        repo_root = Path(__file__).resolve().parents[1]
        content = (repo_root / ".github" / "workflows" / "forecast_data.yml").read_text(encoding="utf-8")

        self.assertIn("FORECAST_DAYS: ${{ vars.FORECAST_DAYS || '3' }}", content)


if __name__ == "__main__":
    unittest.main()
