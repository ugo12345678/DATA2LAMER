from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone

from pscripts.environment.consolidation import consolidate_source_values
from pscripts.environment.entities import SourceValue


class EnvironmentConsolidationTest(unittest.TestCase):
    def setUp(self):
        self.previous_mode = os.environ.get("APP_PROVENANCE_MODE")
        os.environ["APP_PROVENANCE_MODE"] = "compact"

    def tearDown(self):
        if self.previous_mode is None:
            os.environ.pop("APP_PROVENANCE_MODE", None)
        else:
            os.environ["APP_PROVENANCE_MODE"] = self.previous_mode

    def test_averages_numeric_values_and_compacts_provenance(self):
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)
        valid_time = datetime(2026, 5, 14, 12, tzinfo=timezone.utc)
        values = [
            SourceValue("spot-1", "source-a", valid_time, "wind_speed", 4.0, "m/s", run_time, "wind_speed"),
            SourceValue("spot-1", "source-b", valid_time, "wind_speed", 6.0, "m/s", run_time, "wind_speed"),
        ]

        rows = consolidate_source_values(values, run_time)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["wind_speed_ms"], 5.0)
        self.assertEqual(rows[0]["source_count"], 2)
        provenance = rows[0]["provenance"]["wind_speed_ms"]
        self.assertEqual(provenance["sources"], ["source-a", "source-b"])
        self.assertNotIn("values", provenance)

    def test_uses_circular_mean_for_direction_values(self):
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)
        valid_time = datetime(2026, 5, 14, 12, tzinfo=timezone.utc)
        values = [
            SourceValue("spot-1", "source-a", valid_time, "wind_direction", 350.0, "deg", run_time, "wind_direction"),
            SourceValue("spot-1", "source-b", valid_time, "wind_direction", 10.0, "deg", run_time, "wind_direction"),
        ]

        rows = consolidate_source_values(values, run_time)

        self.assertEqual(len(rows), 1)
        direction = rows[0]["wind_direction_deg"]
        self.assertTrue(direction < 1.0 or direction > 359.0)


if __name__ == "__main__":
    unittest.main()

