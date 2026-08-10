locals {
  external_tables = {
    driver_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*drivers.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}"
    }
    laps_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*laps.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}/{driver_number:INTEGER}"
    }
    location_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*locations.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}/{driver_number:INTEGER}"
    }
    meetings_metadata = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/metadata/*meetings.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/metadata/{year:INTEGER}"
    }
    pits_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*pits.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}/{driver_number:INTEGER}"
    }
    race_control_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*race_control.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}"
    }
    session_results_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/session_key=*/session_result.json"
      partition_prefix = null
    }
    sessions_metadata = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/metadata/*sessions.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/metadata/{year:INTEGER}"
    }
    stints_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*stints.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}/{driver_number:INTEGER}"
    }
    telemetry_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*car_data.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}/{driver_number:INTEGER}"
    }
    weather_raw = {
      source_uri       = "gs://${var.raw_bucket_name}/bronze/telemetry/*weather.json"
      partition_prefix = "gs://${var.raw_bucket_name}/bronze/telemetry/{session_key:INTEGER}"
    }
  }
}

resource "google_bigquery_table" "external" {
  for_each = local.external_tables

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.layers["bronze"].dataset_id
  table_id            = each.key
  deletion_protection = true
  description         = "ApexFlow Bronze external table managed by Terraform."

  external_data_configuration {
    autodetect            = false
    source_format         = "NEWLINE_DELIMITED_JSON"
    source_uris           = [each.value.source_uri]
    schema                = file("${path.module}/schemas/${each.key}.json")
    ignore_unknown_values = each.key == "session_results_raw" ? false : true

    dynamic "hive_partitioning_options" {
      for_each = each.value.partition_prefix == null ? [] : [each.value.partition_prefix]
      content {
        mode              = "CUSTOM"
        source_uri_prefix = hive_partitioning_options.value
      }
    }
  }

  depends_on = [google_storage_bucket.raw]
}
