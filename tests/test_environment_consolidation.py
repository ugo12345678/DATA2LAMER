from __future__ import annotations

import os
import gzip
import unittest
from datetime import datetime, timezone

from pscripts.environment.consolidation import consolidate_source_values
from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.r2_storage import R2SourceValueArchive
from pscripts.environment.repositories import Data2LamerForecastRepository


class FailingTable:
    def insert(self, rows):
        return self

    def execute(self):
        raise RuntimeError("No space left on device")


class FailingClient:
    def table(self, name):
        return FailingTable()


class FakeR2Client:
    def __init__(self):
        self.objects = []

    def put_object(self, **kwargs):
        self.objects.append(kwargs)


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

    def test_data2lamer_storage_failure_disables_optional_storage(self):
        repo = Data2LamerForecastRepository(client=FailingClient())
        source = SourceConfig("source-a", "Source A", "provider", "weather")
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)

        run_id = repo.create_run(source, run_time, run_time, run_time)

        self.assertTrue(run_id)
        self.assertFalse(repo.available)
        self.assertIn("No space left on device", repo.disabled_reason)

    def test_r2_archive_writes_source_values_as_gzipped_jsonl(self):
        fake_client = FakeR2Client()
        archive = R2SourceValueArchive(
            bucket="bucket",
            endpoint_url="https://example.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            prefix="test/source_values",
        )
        archive._client = fake_client
        source = SourceConfig("source-a", "Source A", "provider", "weather")
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)
        value = SourceValue(
            "spot-1",
            "source-a",
            run_time,
            "wind_speed",
            4.2,
            "m/s",
            run_time,
            "wind_speed_10m",
            run_id="run-1",
        )

        key = archive.write_source_values(source=source, run_id="run-1", run_time=run_time, values=[value])

        self.assertEqual(key, "test/source_values/run_date=2026-05-14/run_hour=08/source_code=source-a/run-1.jsonl.gz")
        self.assertEqual(len(fake_client.objects), 1)
        body = gzip.decompress(fake_client.objects[0]["Body"]).decode("utf-8")
        self.assertIn('"spot_id":"spot-1"', body)
        self.assertIn('"metric":"wind_speed"', body)


if __name__ == "__main__":
    unittest.main()
