# ApexFlow Terraform foundation

This root codifies the persistent Google Cloud resources used by local Airflow, dbt, and Streamlit:

- the regional Bronze GCS bucket
- Bronze, Silver, and Gold BigQuery datasets
- all eleven Bronze external tables with explicit schemas
- a keyless pipeline service account
- least-privilege storage and BigQuery IAM grants

Local Docker services and Streamlit are intentionally outside Terraform.

## Prerequisites

1. Install Terraform 1.7 or newer.
2. Authenticate Application Default Credentials:

   ```bash
   gcloud auth application-default login
   gcloud config set project apexflow-f1
   ```

3. Ensure your identity can manage Storage, BigQuery, IAM, and project services.

## First plan

The checked-in import blocks adopt the existing bucket, datasets, and external tables. They prevent Terraform from attempting to recreate live data resources.

```bash
cd infra/terraform
terraform init
terraform fmt -check
terraform validate
terraform plan
```

Review the first plan carefully. Expected changes include:

- importing the existing GCS bucket, datasets, and external tables
- adding resource descriptions
- correcting the meetings schema field to `is_cancelled`
- retaining nullable `session_results_raw.number_of_laps`
- creating `apexflow-pipeline@apexflow-f1.iam.gserviceaccount.com`
- granting the pipeline identity object administration, BigQuery job execution, Bronze read access, and Silver/Gold write access

No service-account key is created. For local development, continue using Application Default Credentials. A workload-identity deployment can impersonate this service account later.

## Apply

Only apply after the import plan has been reviewed:

```bash
terraform apply
```

The bucket cannot be force-destroyed, datasets do not delete their contents on destroy, and external tables have deletion protection.

## State

This first foundation deliberately uses local state to avoid a state-bucket bootstrap cycle. Before shared or automated applies, create a dedicated state bucket and migrate with a GCS backend in a separate reviewed change.

## Variables

Defaults match the current development project. Override them with a non-secret `terraform.tfvars` file or `TF_VAR_...` environment variables. Never commit credentials or state.
