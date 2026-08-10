import unittest
from unittest.mock import Mock, patch

from ingestors.season import discover_race_sessions


def meeting(meeting_key=1254):
    return {
        "circuit_key": 10,
        "circuit_short_name": "Melbourne",
        "circuit_type": "Permanent",
        "country_name": "Australia",
        "meeting_key": meeting_key,
        "meeting_name": "Australian Grand Prix",
        "location": "Melbourne",
        "date_start": "2025-03-14T00:00:00Z",
        "date_end": "2025-03-16T06:00:00Z",
        "year": 2025,
        "is_cancelled": False,
    }


def session(session_key, year=2025):
    return {
        "session_key": session_key,
        "session_name": "Race",
        "session_type": "Race",
        "meeting_key": 1254,
        "date_start": "2025-03-16T04:00:00Z",
        "date_end": "2025-03-16T06:00:00Z",
        "year": year,
    }


class SeasonDiscoveryTests(unittest.TestCase):
    @patch("ingestors.season.upload_to_gcs")
    def test_discovers_sorted_unique_races_and_lands_metadata(self, upload):
        client = Mock()
        client.get_meetings.return_value = [meeting()]
        client.get_sessions.return_value = [session(9694), session(9693), session(9694)]

        result = discover_race_sessions(2025, "bucket", client)

        self.assertEqual(result, [9693, 9694])
        client.get_meetings.assert_called_once_with(2025)
        client.get_sessions.assert_called_once_with(2025, session_type="Race")
        self.assertEqual(
            [call.args[1] for call in upload.call_args_list],
            [
                "bronze/metadata/year=2025/meetings.json",
                "bronze/metadata/year=2025/sessions.json",
            ],
        )

    @patch("ingestors.season.upload_to_gcs")
    def test_empty_calendar_fails_without_writing(self, upload):
        client = Mock()
        client.get_meetings.return_value = []
        client.get_sessions.return_value = [session(9693)]

        with self.assertRaisesRegex(RuntimeError, "No meetings"):
            discover_race_sessions(2025, "bucket", client)
        upload.assert_not_called()

    @patch("ingestors.season.upload_to_gcs")
    def test_empty_race_list_fails_without_writing(self, upload):
        client = Mock()
        client.get_meetings.return_value = [meeting()]
        client.get_sessions.return_value = []

        with self.assertRaisesRegex(RuntimeError, "No race sessions"):
            discover_race_sessions(2025, "bucket", client)
        upload.assert_not_called()

    @patch("ingestors.season.upload_to_gcs")
    def test_mismatched_season_fails_before_writing(self, upload):
        client = Mock()
        client.get_meetings.return_value = [meeting()]
        client.get_sessions.return_value = [session(9693, year=2024)]

        with self.assertRaisesRegex(ValueError, "outside requested year"):
            discover_race_sessions(2025, "bucket", client)
        upload.assert_not_called()

    def test_rejects_years_before_openf1_history(self):
        with self.assertRaisesRegex(ValueError, "from 2018"):
            discover_race_sessions(2017, "bucket", Mock())


if __name__ == "__main__":
    unittest.main()
