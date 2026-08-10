import ast
import unittest
from pathlib import Path


class AirflowDagDefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = Path(__file__).parents[1] / "airflow" / "dags" / "apexflow_seasons.py"
        cls.source = cls.path.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_dag_is_manual_and_serial(self):
        dag_call = next(
            node for node in ast.walk(self.tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "dag"
        )
        keywords = {keyword.arg: keyword.value for keyword in dag_call.keywords}
        self.assertIsInstance(keywords["schedule"], ast.Constant)
        self.assertIsNone(keywords["schedule"].value)
        self.assertEqual(keywords["max_active_runs"].value, 1)
        self.assertEqual(keywords["max_active_tasks"].value, 1)

    def test_dag_exposes_validated_year_list_parameter(self):
        self.assertIn('"years": Param(', self.source)
        self.assertIn('type="array"', self.source)
        self.assertIn('"minimum": 2018', self.source)
        self.assertIn('uniqueItems=True', self.source)
        self.assertIn('get_current_context()["params"]["years"]', self.source)

    def test_dag_refreshes_dbt_after_ingestion(self):
        self.assertIn("def refresh_bigquery_models()", self.source)
        self.assertIn('"dbt",', self.source)
        self.assertIn("ingested_sessions >> refresh_bigquery_models()", self.source)

    def test_dag_uses_runtime_dynamic_task_mapping(self):
        expand_calls = [
            node for node in ast.walk(self.tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "expand"
        ]
        self.assertEqual(len(expand_calls), 1)
        self.assertEqual(expand_calls[0].keywords[0].arg, "session_key")


if __name__ == "__main__":
    unittest.main()
