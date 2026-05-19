from __future__ import annotations

import os
import gzip
import json
import sys
import types
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
from pscripts.environment.sync_environment_forecasts import build_sources, environment_forecast_column_counts
from pscripts.environment.sources import cmems
from pscripts.environment.sources import maree_info
from pscripts.environment.sources import open_meteo
from pscripts.environment.sources import shom


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


class FakeShomResponse:
    def __init__(self, payload):
        self._payload = payload
        self.text = json.dumps(payload)
        self.url = "https://maree.shom.fr/spm/coeff?harborName=BREST"

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeShomSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.response


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

    def test_derives_tide_coefficient_from_daily_sea_level_range_when_direct_missing(self):
        previous_derived = os.environ.get("ENABLE_DERIVED_TIDE_COEFFICIENTS")
        previous_unit = os.environ.get("TIDE_COEFFICIENT_RANGE_UNIT_M")
        os.environ["ENABLE_DERIVED_TIDE_COEFFICIENTS"] = "true"
        os.environ["TIDE_COEFFICIENT_RANGE_UNIT_M"] = "6.10"
        run_time = datetime(2026, 5, 14, 0, tzinfo=timezone.utc)
        values = [
            SourceValue(
                "spot-1",
                "open_meteo_marine",
                datetime(2026, 5, 14, hour, tzinfo=timezone.utc),
                "sea_level_height",
                height,
                "m",
                run_time,
                "sea_level_height_msl",
            )
            for hour, height in enumerate([0.0, 1.2, 2.4, 3.66, 2.2, 1.0])
        ]

        try:
            rows = consolidate_source_values(values, run_time)
        finally:
            if previous_derived is None:
                os.environ.pop("ENABLE_DERIVED_TIDE_COEFFICIENTS", None)
            else:
                os.environ["ENABLE_DERIVED_TIDE_COEFFICIENTS"] = previous_derived
            if previous_unit is None:
                os.environ.pop("TIDE_COEFFICIENT_RANGE_UNIT_M", None)
            else:
                os.environ["TIDE_COEFFICIENT_RANGE_UNIT_M"] = previous_unit

        self.assertEqual(len(rows), 6)
        self.assertAlmostEqual(rows[0]["tide_coefficient"], 60.0)
        self.assertEqual(rows[0]["provenance"]["tide_coefficient"]["sources"], ["derived_tide_range"])

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

    def test_shom_coefficients_parser_reads_json_values(self):
        payload = {
            "data": [
                {"date": "2026-05-01", "coefficients": [82, 83]},
                {"datetime": "2026-05-02T06:12:00+02:00", "coefficient": "91"},
            ]
        }

        coefficients = shom.parse_shom_coefficients(payload)

        self.assertEqual(coefficients[datetime(2026, 5, 1).date()], [82, 83])
        self.assertEqual(coefficients[datetime(2026, 5, 2).date()], [91])

    def test_shom_coefficients_parser_reads_portal_matrix_values(self):
        payload = [
            [["69", "74"], ["79", "84"]],
            [["84", "89"]],
        ]

        coefficients = shom.parse_shom_coefficients(payload, datetime(2026, 1, 1).date())

        self.assertEqual(coefficients[datetime(2026, 1, 1).date()], [69, 74])
        self.assertEqual(coefficients[datetime(2026, 1, 2).date()], [79, 84])
        self.assertEqual(coefficients[datetime(2026, 2, 1).date()], [84, 89])

    def test_shom_tide_source_fetches_hourly_coefficients_without_html_scraping(self):
        previous_days = os.environ.get("FORECAST_DAYS")
        previous_timezone = os.environ.get("FORECAST_TARGET_TIMEZONE")
        os.environ["FORECAST_DAYS"] = "1"
        os.environ["FORECAST_TARGET_TIMEZONE"] = "UTC"
        run_time = datetime(2026, 5, 1, 0, tzinfo=timezone.utc)
        spots = __import__("pandas").DataFrame([{"spot_id": "spot-1"}])
        source = shom.ShomTideCoefficientSource()
        source.session = FakeShomSession(
            FakeShomResponse({"data": [{"date": "2026-05-01", "coefficients": [82, 84]}]})
        )

        try:
            values = source.fetch(spots, run_time)
        finally:
            if previous_days is None:
                os.environ.pop("FORECAST_DAYS", None)
            else:
                os.environ["FORECAST_DAYS"] = previous_days
            if previous_timezone is None:
                os.environ.pop("FORECAST_TARGET_TIMEZONE", None)
            else:
                os.environ["FORECAST_TARGET_TIMEZONE"] = previous_timezone

        self.assertEqual(len(values), 24)
        self.assertEqual(values[0].source_code, "shom_tide_coefficients")
        self.assertEqual(values[0].value, 84.0)
        self.assertEqual(
            source.session.calls[0]["url"],
            "https://services.data.shom.fr/b2q8lrcdl4s04cbabsj4nhcb/hdm/spm/coeff",
        )
        self.assertEqual(source.session.calls[0]["params"]["harborName"], "BREST")
        self.assertEqual(source.session.calls[0]["params"]["date"], "2026-01-01")
        self.assertEqual(source.session.calls[0]["params"]["duration"], 365)
        self.assertEqual(source.session.calls[0]["params"]["utc"], "0")
        self.assertEqual(source.session.calls[0]["params"]["correlation"], "1")
        self.assertEqual(source.session.calls[0]["headers"]["Accept"], "application/json")

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

    def test_cmems_sources_use_bounded_default_variable_requests(self):
        source_env_keys = [
            "CMEMS_IBI_WAV_VARIABLES",
            "CMEMS_IBI_PHY_VARIABLES",
            "CMEMS_IBI_BGC_VARIABLES",
        ]
        previous_values = {key: os.environ.get(key) for key in source_env_keys}
        for key in source_env_keys:
            os.environ.pop(key, None)

        try:
            source_variables = {
                cmems.CmemsWavSource().config.code: cmems.CmemsWavSource()._requested_variables(),
                cmems.CmemsPhySource().config.code: cmems.CmemsPhySource()._requested_variables(),
                cmems.CmemsBgcSource().config.code: cmems.CmemsBgcSource()._requested_variables(),
            }
        finally:
            for key, value in previous_values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(source_variables["cmems_ibi_wav"], ["VHM0", "VTM10", "VMDR"])
        self.assertEqual(source_variables["cmems_ibi_phy"], ["thetao", "so", "uo", "vo"])
        self.assertEqual(source_variables["cmems_ibi_bgc"], ["chl", "phyc", "nppv", "zeu"])
        self.assertTrue(all(variables and len(variables) <= 4 for variables in source_variables.values()))

    def test_cmems_fetch_does_not_retry_unfiltered_dataset_by_default(self):
        previous_open_dataset = cmems._open_dataset
        previous_username = os.environ.get("CMEMS_USERNAME")
        previous_password = os.environ.get("CMEMS_PASSWORD")
        previous_fallback = os.environ.get("CMEMS_ALLOW_UNFILTERED_FALLBACK")
        calls = []

        def fake_open_dataset(dataset_id, spots, run_time, select_surface, variables=None):
            calls.append(variables)
            raise RuntimeError("filtered request failed")

        cmems._open_dataset = fake_open_dataset
        os.environ["CMEMS_USERNAME"] = "user"
        os.environ["CMEMS_PASSWORD"] = "password"
        os.environ.pop("CMEMS_ALLOW_UNFILTERED_FALLBACK", None)

        try:
            with self.assertRaises(RuntimeError):
                cmems.CmemsBgcSource().fetch(__import__("pandas").DataFrame(), datetime(2026, 5, 14, tzinfo=timezone.utc))
        finally:
            cmems._open_dataset = previous_open_dataset
            if previous_username is None:
                os.environ.pop("CMEMS_USERNAME", None)
            else:
                os.environ["CMEMS_USERNAME"] = previous_username
            if previous_password is None:
                os.environ.pop("CMEMS_PASSWORD", None)
            else:
                os.environ["CMEMS_PASSWORD"] = previous_password
            if previous_fallback is None:
                os.environ.pop("CMEMS_ALLOW_UNFILTERED_FALLBACK", None)
            else:
                os.environ["CMEMS_ALLOW_UNFILTERED_FALLBACK"] = previous_fallback

        self.assertEqual(calls, [["chl", "phyc", "nppv", "zeu"]])

    def test_cmems_fetch_unfiltered_retry_is_opt_in(self):
        class FakeDataset:
            data_vars = {}

        previous_open_dataset = cmems._open_dataset
        previous_username = os.environ.get("CMEMS_USERNAME")
        previous_password = os.environ.get("CMEMS_PASSWORD")
        previous_fallback = os.environ.get("CMEMS_ALLOW_UNFILTERED_FALLBACK")
        calls = []

        def fake_open_dataset(dataset_id, spots, run_time, select_surface, variables=None):
            calls.append(variables)
            if variables is not None:
                raise RuntimeError("filtered request failed")
            return FakeDataset()

        cmems._open_dataset = fake_open_dataset
        os.environ["CMEMS_USERNAME"] = "user"
        os.environ["CMEMS_PASSWORD"] = "password"
        os.environ["CMEMS_ALLOW_UNFILTERED_FALLBACK"] = "true"

        try:
            values = cmems.CmemsBgcSource().fetch(
                __import__("pandas").DataFrame(),
                datetime(2026, 5, 14, tzinfo=timezone.utc),
            )
        finally:
            cmems._open_dataset = previous_open_dataset
            if previous_username is None:
                os.environ.pop("CMEMS_USERNAME", None)
            else:
                os.environ["CMEMS_USERNAME"] = previous_username
            if previous_password is None:
                os.environ.pop("CMEMS_PASSWORD", None)
            else:
                os.environ["CMEMS_PASSWORD"] = previous_password
            if previous_fallback is None:
                os.environ.pop("CMEMS_ALLOW_UNFILTERED_FALLBACK", None)
            else:
                os.environ["CMEMS_ALLOW_UNFILTERED_FALLBACK"] = previous_fallback

        self.assertEqual(values, [])
        self.assertEqual(calls, [["chl", "phyc", "nppv", "zeu"], None])

    def test_cmems_open_dataset_limits_variables_and_surface_depth(self):
        class FakeDataset:
            def __init__(self):
                self.coords = {"latitude": object(), "longitude": object(), "depth": object()}
                self.dims = {"depth": 1}
                self.renamed = None
                self.indexed = None

            def rename(self, rename_map):
                self.renamed = rename_map
                self.coords["lat"] = self.coords.pop("latitude")
                self.coords["lon"] = self.coords.pop("longitude")
                return self

            def isel(self, indexers):
                self.indexed = indexers
                return self

        calls = []

        def fake_open_dataset(**kwargs):
            calls.append(kwargs)
            return FakeDataset()

        previous_module = sys.modules.get("copernicusmarine")
        previous_username = os.environ.get("CMEMS_USERNAME")
        previous_password = os.environ.get("CMEMS_PASSWORD")
        sys.modules["copernicusmarine"] = types.SimpleNamespace(open_dataset=fake_open_dataset)
        os.environ["CMEMS_USERNAME"] = "user"
        os.environ["CMEMS_PASSWORD"] = "password"
        spots = __import__("pandas").DataFrame(
            [
                {
                    "latitude_min": 48.0,
                    "latitude_max": 48.2,
                    "longitude_min": -4.6,
                    "longitude_max": -4.4,
                }
            ]
        )

        try:
            ds = cmems._open_dataset(
                "cmems_mod_ibi_bgc_anfc_0.027deg-3D_P1D-m",
                spots,
                datetime(2026, 5, 14, tzinfo=timezone.utc),
                True,
                ["chl", "phyc"],
            )
        finally:
            if previous_module is None:
                sys.modules.pop("copernicusmarine", None)
            else:
                sys.modules["copernicusmarine"] = previous_module
            if previous_username is None:
                os.environ.pop("CMEMS_USERNAME", None)
            else:
                os.environ["CMEMS_USERNAME"] = previous_username
            if previous_password is None:
                os.environ.pop("CMEMS_PASSWORD", None)
            else:
                os.environ["CMEMS_PASSWORD"] = previous_password

        self.assertEqual(calls[0]["variables"], ["chl", "phyc"])
        self.assertEqual(calls[0]["minimum_depth"], 0.0)
        self.assertEqual(calls[0]["maximum_depth"], 0.0)
        self.assertEqual(ds.renamed, {"latitude": "lat", "longitude": "lon"})
        self.assertEqual(ds.indexed, {"depth": 0})

    def test_cmems_bgc_adds_plankton_metrics_and_bloom_proxy(self):
        previous_low = os.environ.get("ALGAL_BLOOM_CHL_LOW_MG_M3")
        previous_high = os.environ.get("ALGAL_BLOOM_CHL_HIGH_MG_M3")
        os.environ["ALGAL_BLOOM_CHL_LOW_MG_M3"] = "3"
        os.environ["ALGAL_BLOOM_CHL_HIGH_MG_M3"] = "10"

        try:
            values = cmems.CmemsBgcSource()._metric_values(
                __import__("pandas").Series(
                    {
                        "chlorophyll": 6.5,
                        "phytoplankton_carbon": 12.0,
                        "net_primary_production": 240.0,
                        "euphotic_depth": 18.0,
                    }
                )
            )
        finally:
            if previous_low is None:
                os.environ.pop("ALGAL_BLOOM_CHL_LOW_MG_M3", None)
            else:
                os.environ["ALGAL_BLOOM_CHL_LOW_MG_M3"] = previous_low
            if previous_high is None:
                os.environ.pop("ALGAL_BLOOM_CHL_HIGH_MG_M3", None)
            else:
                os.environ["ALGAL_BLOOM_CHL_HIGH_MG_M3"] = previous_high

        self.assertEqual(values["phytoplankton_carbon"], 12.0)
        self.assertEqual(values["net_primary_production"], 240.0)
        self.assertEqual(values["euphotic_depth"], 18.0)
        self.assertAlmostEqual(values["algal_bloom_risk"], 0.5)

    def test_forecast_sources_allowlist_limits_built_sources(self):
        previous_sources = os.environ.get("FORECAST_SOURCES")
        previous_cmems = os.environ.get("ENABLE_CMEMS")
        previous_metno = os.environ.get("ENABLE_METNO")
        previous_shom = os.environ.get("ENABLE_SHOM_TIDES")
        os.environ["FORECAST_SOURCES"] = "open_meteo_weather,open_meteo_marine"
        os.environ["ENABLE_CMEMS"] = "false"
        os.environ["ENABLE_METNO"] = "false"
        os.environ["ENABLE_SHOM_TIDES"] = "false"

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
            if previous_shom is None:
                os.environ.pop("ENABLE_SHOM_TIDES", None)
            else:
                os.environ["ENABLE_SHOM_TIDES"] = previous_shom

        self.assertEqual(source_codes, ["open_meteo_weather", "open_meteo_marine"])

    def test_environment_forecast_column_counts_counts_non_null_values(self):
        rows = [
            {
                "spot_id": "spot-1",
                "valid_time": "2026-05-14T08:00:00+00:00",
                "wind_speed_ms": 4.2,
                "wave_height_m": None,
                "chlorophyll_mg_m3": 1.1,
            },
            {
                "spot_id": "spot-2",
                "valid_time": "2026-05-14T08:00:00+00:00",
                "wind_speed_ms": 5.3,
                "wave_height_m": 0.8,
            },
        ]

        counts = environment_forecast_column_counts(rows)

        self.assertEqual(counts["spot_id"], 2)
        self.assertEqual(counts["valid_time"], 2)
        self.assertEqual(counts["wind_speed_ms"], 2)
        self.assertEqual(counts["wave_height_m"], 1)
        self.assertEqual(counts["chlorophyll_mg_m3"], 1)
        self.assertEqual(counts["salinity_psu"], 0)


if __name__ == "__main__":
    unittest.main()
