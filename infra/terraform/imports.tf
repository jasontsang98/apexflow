import {
  to = google_storage_bucket.raw
  id = var.raw_bucket_name
}

import {
  to = google_bigquery_dataset.layers["bronze"]
  id = "${var.project_id}/apexflow_bronze"
}

import {
  to = google_bigquery_dataset.layers["silver"]
  id = "${var.project_id}/apexflow_silver"
}

import {
  to = google_bigquery_dataset.layers["gold"]
  id = "${var.project_id}/apexflow_gold"
}

import {
  for_each = local.external_tables
  to       = google_bigquery_table.external[each.key]
  id       = "${var.project_id}/apexflow_bronze/${each.key}"
}
