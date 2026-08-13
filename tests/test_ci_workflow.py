import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


class GitHubActionsWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text()

    def test_workflow_runs_for_main_pull_requests(self):
        self.assertIn("pull_request:", self.workflow)
        self.assertIn("branches: [main]", self.workflow)

    def test_workflow_has_read_only_repository_permissions(self):
        self.assertIn("permissions:\n  contents: read", self.workflow)
        self.assertNotIn("id-token: write", self.workflow)

    def test_python_job_uses_locked_dependencies_and_full_suite(self):
        self.assertIn("uv sync --frozen", self.workflow)
        self.assertIn("python -m unittest discover -s tests -v", self.workflow)

    def test_dbt_check_is_parse_only(self):
        self.assertIn("dbt parse", self.workflow)
        self.assertNotIn("dbt build", self.workflow)
        self.assertNotIn("dbt run", self.workflow)

    def test_terraform_check_never_plans_or_applies(self):
        self.assertIn("terraform fmt -check -recursive", self.workflow)
        self.assertIn("terraform init -backend=false -input=false", self.workflow)
        self.assertIn("terraform validate -no-color", self.workflow)
        self.assertNotIn("terraform plan", self.workflow)
        self.assertNotIn("terraform apply", self.workflow)

    def test_compose_configuration_is_validated_without_starting_services(self):
        self.assertIn(
            "docker compose -f docker-compose.airflow.yml config --quiet",
            self.workflow,
        )
        self.assertNotIn("docker compose up", self.workflow)


if __name__ == "__main__":
    unittest.main()
