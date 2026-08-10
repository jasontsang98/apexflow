import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TERRAFORM_ROOT = ROOT / "infra" / "terraform"
EXPECTED_TABLES = {
    "driver_raw",
    "laps_raw",
    "location_raw",
    "meetings_metadata",
    "pits_raw",
    "race_control_raw",
    "session_results_raw",
    "sessions_metadata",
    "stints_raw",
    "telemetry_raw",
    "weather_raw",
}


class TerraformFoundationTests(unittest.TestCase):
    def test_all_bronze_sources_have_explicit_schemas(self):
        schema_names = {path.stem for path in (TERRAFORM_ROOT / "schemas").glob("*.json")}

        self.assertEqual(schema_names, EXPECTED_TABLES)
        for table_name in schema_names:
            schema = json.loads((TERRAFORM_ROOT / "schemas" / f"{table_name}.json").read_text())
            self.assertTrue(schema, table_name)
            self.assertTrue(all({"name", "type", "mode"} <= set(field) for field in schema))

    def test_sparse_openf1_fields_remain_nullable(self):
        laps = {
            field["name"]: field
            for field in json.loads(
                (TERRAFORM_ROOT / "schemas" / "laps_raw.json").read_text()
            )
        }
        results = {
            field["name"]: field
            for field in json.loads(
                (TERRAFORM_ROOT / "schemas" / "session_results_raw.json").read_text()
            )
        }

        self.assertEqual(laps["date_start"]["mode"], "NULLABLE")
        self.assertEqual(results["number_of_laps"]["mode"], "NULLABLE")

    def test_live_resources_are_adopted_with_import_blocks(self):
        imports = (TERRAFORM_ROOT / "imports.tf").read_text()

        self.assertIn("google_storage_bucket.raw", imports)
        self.assertIn('google_bigquery_dataset.layers["bronze"]', imports)
        self.assertIn("google_bigquery_table.external[each.key]", imports)

    def test_no_service_account_key_resource_is_created(self):
        terraform = "\n".join(
            path.read_text()
            for path in TERRAFORM_ROOT.glob("*.tf")
        )

        self.assertNotIn("google_service_account_key", terraform)


if __name__ == "__main__":
    unittest.main()
