import unittest

import pandas as pd

from dashboard.charts import (
    fastest_lap_chart,
    lap_delta_chart,
    season_driver_chart,
    season_races_chart,
    telemetry_chart,
    tire_degradation_chart,
    v_min_track_chart,
)


class DashboardChartTests(unittest.TestCase):
    def setUp(self):
        self.laps = pd.DataFrame([
            {
                "driver_number": 4, "name_acronym": "NOR", "full_name": "Lando Norris",
                "team_name": "McLaren", "team_colour_hex": "#FF8000", "lap_number": 10,
                "lap_duration": 81.2, "delta_to_driver_best": 0.2, "tire_compound": "MEDIUM",
                "tire_age_at_lap_end": 8,
            },
            {
                "driver_number": 1, "name_acronym": "VER", "full_name": "Max Verstappen",
                "team_name": "Red Bull Racing", "team_colour_hex": "#3671C6", "lap_number": 10,
                "lap_duration": 80.9, "delta_to_driver_best": 0.1, "tire_compound": "HARD",
                "tire_age_at_lap_end": 9,
            },
        ])

    def test_fastest_lap_chart_contains_one_bar_trace_per_colour(self):
        figure = fastest_lap_chart(self.laps)
        self.assertGreaterEqual(len(figure.data), 1)
        self.assertEqual(figure.layout.showlegend, False)

    def test_lap_delta_chart_contains_driver_traces(self):
        figure = lap_delta_chart(self.laps)
        self.assertEqual(len(figure.data), 2)

    def test_tire_degradation_chart(self):
        tires = self.laps.assign(
            lap_in_stint=[1, 1],
            delta_to_stint_best=[0.0, 0.0],
            strategy_stint_number=[1, 1],
        )
        figure = tire_degradation_chart(tires)
        self.assertEqual(len(figure.data), 2)

    def test_season_races_chart(self):
        races = pd.DataFrame([
            {
                "session_start": "2025-03-16", "meeting_name": "Australian Grand Prix",
                "circuit_short_name": "Melbourne", "fastest_driver": "NOR",
                "fastest_lap": 79.8, "lap_count": 921, "driver_count": 17, "was_wet": True,
            },
            {
                "session_start": "2025-03-23", "meeting_name": "Chinese Grand Prix",
                "circuit_short_name": "Shanghai", "fastest_driver": "VER",
                "fastest_lap": 92.1, "lap_count": 850, "driver_count": 20, "was_wet": False,
            },
        ])
        figure = season_races_chart(races)
        self.assertEqual(len(figure.data), 2)

    def test_season_driver_chart(self):
        drivers = pd.DataFrame([
            {
                "name_acronym": "NOR", "full_name": "Lando Norris", "team_name": "McLaren",
                "team_colour_hex": "#FF8000", "races": 2, "laps": 110,
                "average_delta_to_best": 1.2, "top_speed": 315,
            },
            {
                "name_acronym": "VER", "full_name": "Max Verstappen", "team_name": "Red Bull Racing",
                "team_colour_hex": "#3671C6", "races": 2, "laps": 112,
                "average_delta_to_best": 1.0, "top_speed": 318,
            },
        ])
        figure = season_driver_chart(drivers)
        self.assertGreaterEqual(len(figure.data), 1)
        self.assertEqual(figure.layout.showlegend, False)

    def test_v_min_track_chart_locks_track_aspect_ratio(self):
        points = self.laps.assign(x=[10.0, 20.0], y=[30.0, 40.0], lap_v_min=[85, 90], v_min_gear=[3, 4])
        figure = v_min_track_chart(points)
        self.assertEqual(figure.layout.xaxis.scaleanchor, "y")

    def test_telemetry_chart_contains_speed_throttle_and_brake(self):
        telemetry = pd.DataFrame({
            "telemetry_timestamp": ["2025-01-01T00:00:00Z", "2025-01-01T00:00:01Z"],
            "speed": [100, 120],
            "throttle": [50, 100],
            "brake": [1, 0],
        })
        figure = telemetry_chart(telemetry)
        self.assertEqual([trace.name for trace in figure.data], ["Speed", "Throttle", "Brake"])


if __name__ == "__main__":
    unittest.main()
