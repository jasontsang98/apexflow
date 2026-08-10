import os
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from dashboard.data import DashboardRepository, dashboard_project


class DashboardProjectTests(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_default_project(self):
        self.assertEqual(dashboard_project(), "apexflow-f1")

    @patch.dict(os.environ, {"APEXFLOW_BQ_PROJECT": "valid-project-123"}, clear=True)
    def test_environment_project(self):
        self.assertEqual(dashboard_project(), "valid-project-123")

    @patch.dict(os.environ, {"APEXFLOW_BQ_PROJECT": "invalid.project"}, clear=True)
    def test_invalid_environment_project(self):
        with self.assertRaisesRegex(ValueError, "valid Google Cloud project"):
            dashboard_project()


class DashboardRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.result = pd.DataFrame({"value": [1]})
        self.client.query.return_value.to_dataframe.return_value = self.result
        self.repository = DashboardRepository(client=self.client, project="apexflow-f1")

    def test_invalid_explicit_project(self):
        with self.assertRaises(ValueError):
            DashboardRepository(client=self.client, project="bad")

    def test_sessions_queries_gold_dimension(self):
        result = self.repository.sessions()

        self.assertIs(result, self.result)
        sql = self.client.query.call_args.args[0]
        self.assertIn("apexflow_gold.dim_sessions", sql)

    def test_season_races_uses_year_parameter(self):
        self.repository.season_races(2025)

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("fct_dashboard_laps", sql)
        self.assertIn("dim_sessions", sql)
        self.assertEqual(config.query_parameters[0].value, 2025)

    def test_season_drivers_uses_year_parameter(self):
        self.repository.season_drivers(2025)

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("average_delta_to_best", sql)
        self.assertEqual(config.query_parameters[0].value, 2025)

    def test_season_winners_uses_year_parameter(self):
        self.repository.season_winners(2025)

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("fct_race_results", sql)
        self.assertIn("COUNTIF(finishing_position = 1)", sql)
        self.assertEqual(config.query_parameters[0].value, 2025)

    def test_race_results_uses_session_parameter(self):
        self.repository.race_results(9693)

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("fct_race_results", sql)
        self.assertIn("ORDER BY finishing_position", sql)
        self.assertEqual(config.query_parameters[0].value, 9693)

    def test_drivers_uses_session_parameter(self):
        self.repository.drivers(9693)

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("apexflow_gold.dim_drivers", sql)
        self.assertEqual(config.query_parameters[0].value, 9693)

    def test_laps_skips_query_for_empty_driver_selection(self):
        result = self.repository.laps(9693, [])

        self.assertTrue(result.empty)
        self.client.query.assert_not_called()

    def test_laps_uses_scalar_and_array_parameters(self):
        self.repository.laps(9693, [1, 4])

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("fct_dashboard_laps", sql)
        self.assertEqual(config.query_parameters[0].value, 9693)
        self.assertEqual(config.query_parameters[1].values, [1, 4])

    def test_tire_and_v_min_queries_skip_empty_selection(self):
        self.assertTrue(self.repository.tire_degradation(9693, []).empty)
        self.assertTrue(self.repository.v_min_points(9693, []).empty)
        self.client.query.assert_not_called()

    def test_tire_query_uses_selected_drivers(self):
        self.repository.tire_degradation(9693, [1, 4])

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("fct_tire_degradation", sql)
        self.assertEqual(config.query_parameters[1].values, [1, 4])

    def test_v_min_query_uses_selected_drivers(self):
        self.repository.v_min_points(9693, [4])

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("fct_corner_performance", sql)
        self.assertEqual(config.query_parameters[0].value, 9693)
        self.assertEqual(config.query_parameters[1].values, [4])

    def test_telemetry_query_is_fully_parameterized(self):
        self.repository.telemetry(9693, 4, 57)

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("fct_telemetry_enriched", sql)
        self.assertEqual([parameter.value for parameter in config.query_parameters], [9693, 4, 57])

    def test_fastest_lap_telemetry_skips_empty_selection(self):
        result = self.repository.fastest_lap_telemetry(9693, [])

        self.assertTrue(result.empty)
        self.client.query.assert_not_called()

    def test_fastest_lap_telemetry_uses_selected_drivers(self):
        self.repository.fastest_lap_telemetry(9693, [1, 4])

        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("ARRAY_AGG", sql)
        self.assertIn("AND NOT is_pit_out_lap", sql)
        self.assertIn("fct_telemetry_enriched", sql)
        self.assertEqual(config.query_parameters[0].value, 9693)
        self.assertEqual(config.query_parameters[1].values, [1, 4])



if __name__ == "__main__":
    unittest.main()
