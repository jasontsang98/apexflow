import json
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from pydantic import ValidationError

from ingestors.main import (
    ingest_driver_laps,
    ingest_driver_locations,
    ingest_driver_pit_stops,
    ingest_driver_stints,
    ingest_driver_telemetry,
    run_ingestion,
    upload_to_gcs,
)
from ingestors.schemas import LapData, TelemetryData


class UploadTests(unittest.TestCase):
    @patch("ingestors.main.storage.Client")
    def test_upload_serializes_dicts_and_models_as_ndjson(self, client_class):
        blob = client_class.return_value.bucket.return_value.blob.return_value
        telemetry = TelemetryData(
            date=datetime(2025, 3, 16, tzinfo=timezone.utc),
            session_key=9693,
            meeting_key=1254,
            driver_number=4,
            speed=301,
            rpm=12000,
            n_gear=8,
            throttle=100,
            brake=0,
            drs=12,
        )

        uri = upload_to_gcs("bucket", "path/data.json", [telemetry, {"value": 1}])

        self.assertEqual(uri, "gs://bucket/path/data.json")
        uploaded = blob.upload_from_string.call_args.kwargs
        lines = uploaded["data"].splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["speed"], 301)
        self.assertEqual(json.loads(lines[1]), {"value": 1})
        self.assertEqual(uploaded["content_type"], "application/x-ndjson")

    @patch("ingestors.main.storage.Client")
    def test_upload_failure_propagates(self, client_class):
        blob = client_class.return_value.bucket.return_value.blob.return_value
        blob.upload_from_string.side_effect = OSError("write failed")

        with self.assertRaisesRegex(OSError, "write failed"):
            upload_to_gcs("bucket", "path", [{"value": 1}])


class DriverIngestionTests(unittest.TestCase):
    @patch("ingestors.main.upload_to_gcs")
    def test_empty_api_result_is_not_uploaded(self, upload):
        client = Mock()
        client.get_car_data.return_value = []

        result = ingest_driver_telemetry(client, "bucket", 9693, 4)

        self.assertFalse(result)
        upload.assert_not_called()

    @patch("ingestors.main.upload_to_gcs")
    def test_valid_telemetry_uses_partitioned_destination(self, upload):
        client = Mock()
        client.get_car_data.return_value = [{
            "date": "2025-03-16T04:00:00Z",
            "session_key": 9693,
            "meeting_key": 1254,
            "driver_number": 4,
            "speed": 301,
            "rpm": 12000,
            "n_gear": 8,
            "throttle": 100,
            "brake": 0,
            "drs": 12,
        }]

        result = ingest_driver_telemetry(client, "bucket", 9693, 4)

        self.assertTrue(result)
        args = upload.call_args.args
        self.assertEqual(args[:2], (
            "bucket",
            "bronze/telemetry/session_key=9693/driver_number=4/car_data.json",
        ))
        self.assertIsInstance(args[2][0], TelemetryData)

    @patch("ingestors.main.upload_to_gcs")
    def test_invalid_telemetry_fails_before_upload(self, upload):
        client = Mock()
        client.get_car_data.return_value = [{"speed": 301}]

        with self.assertRaises(ValidationError):
            ingest_driver_telemetry(client, "bucket", 9693, 4)
        upload.assert_not_called()

    @patch("ingestors.main._ingest_driver_data")
    def test_dataset_wrappers_supply_schema_and_filename(self, ingest):
        client = Mock()
        cases = (
            (ingest_driver_laps, client.get_car_laps, "laps.json"),
            (ingest_driver_stints, client.get_stints, "stints.json"),
            (ingest_driver_pit_stops, client.get_pit_stops, "pits.json"),
            (ingest_driver_locations, client.get_locations, "locations.json"),
        )

        for wrapper, fetch, filename in cases:
            with self.subTest(filename=filename):
                ingest.reset_mock()
                wrapper(client, "bucket", 9693, 4)
                called = ingest.call_args.args
                self.assertEqual(called[0], fetch)
                self.assertEqual(called[2:], (filename, "bucket", 9693, 4))

    def test_lap_schema_removes_null_sector_segments(self):
        lap = LapData(
            session_key=9693,
            meeting_key=1254,
            driver_number=4,
            lap_number=1,
            date_start="2025-03-16T04:00:00Z",
            is_pit_out_lap=False,
            segments_sector_1=[1.0, None, 2.0],
            segments_sector_2=None,
        )

        self.assertEqual(lap.segments_sector_1, [1.0, 2.0])
        self.assertEqual(lap.segments_sector_2, [])


class RunIngestionTests(unittest.TestCase):
    @patch.dict("os.environ", {
        "APEXFLOW_BUCKET": "test-bucket",
        "APEXFLOW_SESSION_KEY": "42",
        "OPENF1_REQUEST_DELAY": "0",
        "OPENF1_REQUEST_TIMEOUT": "5",
        "OPENF1_MAX_RETRIES": "1",
    }, clear=True)
    @patch("ingestors.main.ingest_driver_locations")
    @patch("ingestors.main.ingest_driver_pit_stops")
    @patch("ingestors.main.ingest_driver_stints")
    @patch("ingestors.main.ingest_driver_laps")
    @patch("ingestors.main.ingest_driver_telemetry")
    @patch("ingestors.main.OpenF1Client")
    def test_runtime_configuration_and_all_driver_tasks(
        self,
        client_class,
        telemetry,
        laps,
        stints,
        pits,
        locations,
    ):
        client = client_class.return_value
        client.get_drivers.return_value = [{
            "driver_number": 4,
            "broadcast_name": "L NORRIS",
            "full_name": "Lando Norris",
            "name_acronym": "NOR",
            "team_name": "McLaren",
            "team_colour": "FF8000",
            "session_key": 42,
        }]

        run_ingestion()

        client_class.assert_called_once_with(
            sustained_delay=0.0,
            timeout=5.0,
            max_retries=1,
        )
        for task in (telemetry, laps, stints, pits, locations):
            task.assert_called_once_with(client, "test-bucket", 42, 4)

    @patch("ingestors.main.OpenF1Client")
    def test_no_drivers_is_a_pipeline_failure(self, client_class):
        client_class.return_value.get_drivers.return_value = []

        with self.assertRaisesRegex(RuntimeError, "No drivers"):
            run_ingestion()


if __name__ == "__main__":
    unittest.main()
