import unittest

import pandas as pd

from dashboard.analytics import (
    add_elapsed_time,
    fastest_laps,
    format_lap_time,
    overview_metrics,
    safe_axis_range,
    season_metrics,
)


class DashboardAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.laps = pd.DataFrame([
            {"driver_number": 4, "name_acronym": "NOR", "lap_duration": 81.234, "top_speed": 312},
            {"driver_number": 4, "name_acronym": "NOR", "lap_duration": 80.500, "top_speed": 315},
            {"driver_number": 1, "name_acronym": "VER", "lap_duration": 80.750, "top_speed": 318},
        ])

    def test_format_lap_time(self):
        self.assertEqual(format_lap_time(80.5), "1:20.500")
        self.assertEqual(format_lap_time(None), "—")
        self.assertEqual(format_lap_time(float("nan")), "—")

    def test_fastest_laps_returns_one_row_per_driver(self):
        result = fastest_laps(self.laps)

        self.assertEqual(result["name_acronym"].tolist(), ["NOR", "VER"])
        self.assertEqual(result["lap_duration"].tolist(), [80.5, 80.75])

    def test_fastest_laps_preserves_empty_frame(self):
        self.assertTrue(fastest_laps(pd.DataFrame()).empty)

    def test_overview_metrics(self):
        result = overview_metrics(self.laps)

        self.assertEqual(result, {
            "fastest_lap": "1:20.500",
            "fastest_driver": "NOR",
            "top_speed": "318 km/h",
            "lap_count": "3",
        })

    def test_overview_metrics_handles_empty_data(self):
        self.assertEqual(overview_metrics(pd.DataFrame())["lap_count"], "0")

    def test_season_metrics(self):
        races = pd.DataFrame([
            {"fastest_lap": 80.5, "fastest_driver": "NOR", "lap_count": 921},
            {"fastest_lap": 91.1, "fastest_driver": "VER", "lap_count": 850},
        ])
        drivers = pd.DataFrame({"driver_number": [1, 4, 81]})

        result = season_metrics(races, drivers)

        self.assertEqual(result, {
            "race_count": "2",
            "driver_count": "3",
            "lap_count": "1,771",
            "fastest_lap": "1:20.500",
            "fastest_driver": "NOR",
        })

    def test_season_metrics_handles_empty_data(self):
        result = season_metrics(pd.DataFrame(), pd.DataFrame())

        self.assertEqual(result, {
            "race_count": "0",
            "driver_count": "0",
            "lap_count": "0",
            "fastest_lap": "—",
            "fastest_driver": "—",
        })

    def test_add_elapsed_time_uses_first_timestamp(self):
        telemetry = pd.DataFrame({
            "telemetry_timestamp": ["2025-01-01T00:00:01Z", "2025-01-01T00:00:03.5Z"]
        })

        result = add_elapsed_time(telemetry)

        self.assertEqual(result["elapsed_seconds"].tolist(), [0.0, 2.5])

    def test_add_elapsed_time_handles_empty_frame(self):
        result = add_elapsed_time(pd.DataFrame())

        self.assertIn("elapsed_seconds", result)
        self.assertTrue(result.empty)

    def test_safe_axis_range(self):
        self.assertEqual(safe_axis_range(pd.Series([10, 10])), (9.0, 11.0))
        self.assertEqual(safe_axis_range(pd.Series(dtype=float)), None)
        self.assertEqual(safe_axis_range(pd.Series([0, 100])), (-5.0, 105.0))


if __name__ == "__main__":
    unittest.main()
