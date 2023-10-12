provider "google" {
  credentials = file(var.credentials_path)
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "storage_bucket" {
  project                  = var.project_id
  for_each                 = toset(var.bucket_name_set)
  name                     = each.value
  location                 = var.region
  force_destroy            = true #uncomment to fully clean working folders
  storage_class            = "STANDARD"
  public_access_prevention = "enforced"

}

resource "google_bigquery_dataset" "dataset" {
  project                     = var.project_id
  dataset_id                  = var.dataset_id
  friendly_name               = "avro_dataset"
  description                 = "dataset used to store avro files"
  location                    = var.region
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.table_id

  time_partitioning {
    type = "DAY"
  }

  labels = {
    env = "default"
  }
  deletion_protection = false

}

resource "google_pubsub_topic" "pubsub_topic" {
  name = "avrofilecreated"
  project = var.project_id

  message_retention_duration = "86600s"
}