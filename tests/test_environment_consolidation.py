from __future__ import annotations

import os
import gzip
import unittest
from datetime import datetime, timezone
from io import BytesIO

from pscripts.environment.consolidation import consolidate_source_values
from pscripts.environment.entities import SourceConfig, SourceValue
from pscripts.environment.r2_storage import R2SourceValueArchive, R2TrainingDatasetArchive
from pscripts.environment.repositories import (
    Data2LamerForecastRepository,
    Vu2LamerDiveTrainingDatasetRepository,
    Vu2LamerForecastRepository,
)
from pscripts.environment.sync_environment_forecasts import build_sources
from pscripts.environment.sources import maree_info
from pscripts.environment.sources import open_meteo


class FailingTable:
    def insert(self, rows):
        return self

    def execute(self):
        raise RuntimeError("No space left on device")


class FailingClient:
    def table(self, name):
        return FailingTable()


class FakeResponse:
    def __init__(self, data=None, count=None):
        self.data = data or []
        self.count = count


class FakeForecastTable:
    def __init__(self):
        self.deleted_column = None
        self.deleted_cutoff = None

    def delete(self, **kwargs):
        return self

    def lt(self, column, value):
        self.deleted_column = column
        self.deleted_cutoff = value
        return self

    def execute(self):
        return FakeResponse(count=3)


class FakeForecastClient:
    def __init__(self):
        self.forecast_table = FakeForecastTable()

    def table(self, name):
        return self.forecast_table


class FakeDatasetTable:
    def __init__(self, rows):
        self.rows = rows
        self.range_start = 0
        self.range_end = 0

    def select(self, columns):
        return self

    def order(self, column):
        return self

    def range(self, start, end):
        self.range_start = start
        self.range_end = end
        return self

    def execute(self):
        return FakeResponse(data=self.rows[self.range_start : self.range_end + 1])


class FakeDatasetClient:
    def __init__(self, rows):
        self.rows = rows

    def table(self, name):
        return FakeDatasetTable(self.rows)


class FakeR2Client:
    def __init__(self):
        self.objects = []

    def put_object(self, **kwargs):
        self.objects.append(kwargs)

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise ValueError(name)
        return FakeR2Paginator(self.objects)

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        for item in self.objects:
            if item["Key"] == key:
                return {"Body": BytesIO(item["Body"])}
        raise KeyError(key)


class FakeR2Paginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, **kwargs):
        prefix = kwargs["Prefix"]
        yield {
            "Contents": [
                {"Key": item["Key"]}
                for item in self.objects
                if item["Key"].startswith(prefix)
            ]
        }


class FakeOpenMeteoResponse:
    def __init__(self, status_code, text="", headers=None, payload=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeOpenMeteoSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        return self.responses.pop(0)


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

    def test_consolidates_tide_coefficient_metric(self):
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)
        valid_time = datetime(2026, 5, 14, 12, tzinfo=timezone.utc)
        values = [
            SourceValue(
                "spot-1",
                "maree_info_tide_coefficients",
                valid_time,
                "tide_coefficient",
                77.0,
                "coef",
                run_time,
                "daily_tide_coefficient_max",
            ),
        ]

        rows = consolidate_source_values(values, run_time)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["tide_coefficient"], 77.0)
        self.assertEqual(rows[0]["provenance"]["tide_coefficient"]["unit"], "coef")

    def test_maree_info_coefficients_parser_reads_daily_values(self):
        document = """
        <table>
          <tr><th colspan="8">Mai 2026</th></tr>
          <tr><td></td><td>01 V</td><td>Fete du travail</td><td></td><td></td><td></td><td>82</td><td>82</td></tr>
          <tr><td></td><td>17 D</td><td>Pascal</td><td></td><td></td><td></td><td>98</td><td>99</td></tr>
        </table>
        """

        coefficients = maree_info.parse_maree_info_coefficients(document)

        self.assertEqual(coefficients[datetime(2026, 5, 1).date()], [82, 82])
        self.assertEqual(coefficients[datetime(2026, 5, 17).date()], [98, 99])

    def test_maree_info_coefficients_parser_reads_flat_calendar_text(self):
        document = """
        <h2>Coefficients des marees 2026 Brest</h2>
        Mai 2026
        01 V
        Fete du travail
        82 82
        17 D
        Pascal
        98 99
        Afficher les dates des coefficients de maree
        120 95 70
        """

        coefficients = maree_info.parse_maree_info_coefficients(document)

        self.assertEqual(coefficients[datetime(2026, 5, 1).date()], [82, 82])
        self.assertEqual(coefficients[datetime(2026, 5, 17).date()], [98, 99])

    def test_data2lamer_storage_failure_disables_optional_storage(self):
        repo = Data2LamerForecastRepository(client=FailingClient())
        source = SourceConfig("source-a", "Source A", "provider", "weather")
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)

        run_id = repo.create_run(source, run_time, run_time, run_time)

        self.assertTrue(run_id)
        self.assertFalse(repo.available)
        self.assertIn("No space left on device", repo.disabled_reason)

    def test_forecast_delete_expired_uses_explicit_cutoff(self):
        client = FakeForecastClient()
        repo = Vu2LamerForecastRepository(client=client)
        cutoff = datetime(2026, 5, 14, 8, 12, tzinfo=timezone.utc)

        deleted = repo.delete_expired(cutoff=cutoff)

        self.assertEqual(deleted, 3)
        self.assertEqual(client.forecast_table.deleted_column, "valid_time")
        self.assertEqual(client.forecast_table.deleted_cutoff, "2026-05-14T08:12:00+00:00")

    def test_training_dataset_repository_fetches_paginated_rows(self):
        previous_batch_size = os.environ.get("TRAINING_DATASET_FETCH_BATCH_SIZE")
        os.environ["TRAINING_DATASET_FETCH_BATCH_SIZE"] = "2"
        rows = [{"outing_id": "1"}, {"outing_id": "2"}, {"outing_id": "3"}]

        try:
            repo = Vu2LamerDiveTrainingDatasetRepository(client=FakeDatasetClient(rows))
            fetched = repo.fetch_rows()
        finally:
            if previous_batch_size is None:
                os.environ.pop("TRAINING_DATASET_FETCH_BATCH_SIZE", None)
            else:
                os.environ["TRAINING_DATASET_FETCH_BATCH_SIZE"] = previous_batch_size

        self.assertEqual(fetched, rows)

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

    def test_r2_archive_lists_latest_run_and_reads_source_values(self):
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
        latest_run_time, keys = archive.latest_source_value_keys(
            lookback_hours=2,
            now=datetime(2026, 5, 14, 10, tzinfo=timezone.utc),
        )
        values = archive.read_source_values(key)

        self.assertEqual(latest_run_time, run_time)
        self.assertEqual(keys, [key])
        self.assertEqual(values[0].spot_id, "spot-1")
        self.assertEqual(values[0].valid_time, run_time)

    def test_r2_training_dataset_archive_writes_jsonl(self):
        fake_client = FakeR2Client()
        archive = R2TrainingDatasetArchive(
            bucket="bucket",
            endpoint_url="https://example.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            prefix="test/training",
        )
        archive._client = fake_client
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)

        result = archive.merge_and_write_rows(
            run_time=run_time,
            rows=[
                {
                    "spot_id": "spot-1",
                    "outing_id": "outing-1",
                    "longitude": -4.49,
                    "latitude": 48.39,
                    "observed_visibility_m": 8,
                }
            ],
        )

        self.assertEqual(result["latest_key"], "test/training/latest.jsonl.gz")
        self.assertEqual(result["run_key"], "test/training/runs/run_date=2026-05-14/run_hour=08/dataset_delta.jsonl.gz")
        body = gzip.decompress(fake_client.objects[0]["Body"]).decode("utf-8")
        self.assertIn('"observed_visibility_m":8', body)

    def test_r2_training_dataset_archive_merges_by_outing_id(self):
        fake_client = FakeR2Client()
        archive = R2TrainingDatasetArchive(
            bucket="bucket",
            endpoint_url="https://example.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            prefix="test/training",
        )
        archive._client = fake_client
        run_time = datetime(2026, 5, 14, 8, tzinfo=timezone.utc)

        archive.merge_and_write_rows(
            run_time=run_time,
            rows=[{"outing_id": "outing-1", "observed_visibility_m": 8}],
        )
        result = archive.merge_and_write_rows(
            run_time=run_time,
            rows=[{"outing_id": "outing-1", "observed_visibility_m": 12}],
        )

        latest_object = [item for item in fake_client.objects if item["Key"] == "test/training/latest.jsonl.gz"][-1]
        latest_body = gzip.decompress(latest_object["Body"]).decode("utf-8").splitlines()
        self.assertEqual(result["rows_count"], 1)
        self.assertEqual(len(latest_body), 1)
        self.assertIn('"observed_visibility_m":12', latest_body[0])

    def test_open_meteo_hourly_rate_limit_stops_without_retrying(self):
        previous_cooldown = os.environ.get("OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC")
        previous_interval = os.environ.get("OPEN_METEO_MIN_REQUEST_INTERVAL_SEC")
        os.environ["OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC"] = "1"
        os.environ["OPEN_METEO_MIN_REQUEST_INTERVAL_SEC"] = "0"
        open_meteo.OPEN_METEO_RATE_LIMITER = open_meteo._OpenMeteoHostRateLimiter()
        session = FakeOpenMeteoSession(
            [
                FakeOpenMeteoResponse(
                    429,
                    '{"error":true,"reason":"Hourly API request limit exceeded. Please try again in the next hour."}',
                )
            ]
        )

        try:
            with self.assertRaises(open_meteo.OpenMeteoRateLimitError):
                open_meteo._get_with_retry(session, "https://api.open-meteo.com/v1/forecast", {})
        finally:
            if previous_cooldown is None:
                os.environ.pop("OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC", None)
            else:
                os.environ["OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC"] = previous_cooldown
            if previous_interval is None:
                os.environ.pop("OPEN_METEO_MIN_REQUEST_INTERVAL_SEC", None)
            else:
                os.environ["OPEN_METEO_MIN_REQUEST_INTERVAL_SEC"] = previous_interval

        self.assertEqual(session.calls, 1)

    def test_open_meteo_rate_limit_blocks_same_host_for_other_sources(self):
        previous_cooldown = os.environ.get("OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC")
        previous_interval = os.environ.get("OPEN_METEO_MIN_REQUEST_INTERVAL_SEC")
        os.environ["OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC"] = "60"
        os.environ["OPEN_METEO_MIN_REQUEST_INTERVAL_SEC"] = "0"
        open_meteo.OPEN_METEO_RATE_LIMITER = open_meteo._OpenMeteoHostRateLimiter()
        first_session = FakeOpenMeteoSession(
            [
                FakeOpenMeteoResponse(
                    429,
                    '{"error":true,"reason":"Hourly API request limit exceeded. Please try again in the next hour."}',
                )
            ]
        )
        second_session = FakeOpenMeteoSession([FakeOpenMeteoResponse(200, payload={"hourly": {}})])

        try:
            with self.assertRaises(open_meteo.OpenMeteoRateLimitError):
                open_meteo._get_with_retry(first_session, "https://api.open-meteo.com/v1/forecast", {})
            with self.assertRaises(open_meteo.OpenMeteoRateLimitBlockedError):
                open_meteo._get_with_retry(second_session, "https://api.open-meteo.com/v1/gfs", {})
        finally:
            if previous_cooldown is None:
                os.environ.pop("OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC", None)
            else:
                os.environ["OPEN_METEO_HOURLY_RATE_LIMIT_COOLDOWN_SEC"] = previous_cooldown
            if previous_interval is None:
                os.environ.pop("OPEN_METEO_MIN_REQUEST_INTERVAL_SEC", None)
            else:
                os.environ["OPEN_METEO_MIN_REQUEST_INTERVAL_SEC"] = previous_interval

        self.assertEqual(first_session.calls, 1)
        self.assertEqual(second_session.calls, 0)

    def test_forecast_sources_allowlist_limits_built_sources(self):
        previous_sources = os.environ.get("FORECAST_SOURCES")
        previous_cmems = os.environ.get("ENABLE_CMEMS")
        previous_metno = os.environ.get("ENABLE_METNO")
        os.environ["FORECAST_SOURCES"] = "open_meteo_weather,open_meteo_marine"
        os.environ["ENABLE_CMEMS"] = "false"
        os.environ["ENABLE_METNO"] = "false"

        try:
            source_codes = [source.config.code for source in build_sources()]
        finally:
            if previous_sources is None:
                os.environ.pop("FORECAST_SOURCES", None)
            else:
                os.environ["FORECAST_SOURCES"] = previous_sources
            if previous_cmems is None:
                os.environ.pop("ENABLE_CMEMS", None)
            else:
                os.environ["ENABLE_CMEMS"] = previous_cmems
            if previous_metno is None:
                os.environ.pop("ENABLE_METNO", None)
            else:
                os.environ["ENABLE_METNO"] = previous_metno

        self.assertEqual(source_codes, ["open_meteo_weather", "open_meteo_marine"])


if __name__ == "__main__":
    unittest.main()
