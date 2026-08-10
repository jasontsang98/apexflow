resource "google_project_service" "required" {
  for_each = toset([
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "storage.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "raw" {
  name                        = var.raw_bucket_name
  project                     = var.project_id
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset" "layers" {
  for_each = {
    bronze = "apexflow_bronze"
    silver = "apexflow_silver"
    gold   = "apexflow_gold"
  }

  project                    = var.project_id
  dataset_id                 = each.value
  location                   = var.region
  delete_contents_on_destroy = false
  description                = "ApexFlow ${title(each.key)} data layer managed by Terraform."

  depends_on = [google_project_service.required]
}

resource "google_service_account" "pipeline" {
  project      = var.project_id
  account_id   = var.pipeline_service_account_id
  display_name = "ApexFlow pipeline"
  description  = "Identity used by Airflow ingestion and dbt transformations."

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket_iam_member" "pipeline_objects" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_bronze_reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.layers["bronze"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_transform_writer" {
  for_each = toset(["silver", "gold"])

  project    = var.project_id
  dataset_id = google_bigquery_dataset.layers[each.value].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}
