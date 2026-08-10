variable "project_id" {
  description = "Google Cloud project that hosts ApexFlow."
  type        = string
  default     = "apexflow-f1"
}

variable "region" {
  description = "Regional location shared by GCS and BigQuery."
  type        = string
  default     = "australia-southeast1"
}

variable "raw_bucket_name" {
  description = "Globally unique bucket containing Bronze objects."
  type        = string
  default     = "apexflow-raw-data"
}

variable "pipeline_service_account_id" {
  description = "Account ID for Airflow ingestion and dbt execution."
  type        = string
  default     = "apexflow-pipeline"
}
