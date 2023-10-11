provider "google" {
  credentials = file(var.credentials_path)
  project     = var.project_id
  region      = var.region
}

# could be skipped, but it seem to solved ZONE_RESOURCE_POOL_EXHAUSTED for me
resource "google_compute_instance" "default" {
  name         = "dataflow-worker"
  machine_type = "n1-standard-1"
  zone         = "us-east1-b"

  tags = ["dev"]

  scheduling {
    automatic_restart  = false
    preemptible        = true
    provisioning_model = "SPOT"
  }

  boot_disk {
    initialize_params {
      image = var.image_name
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral IP
    }

  }
}
resource "google_storage_bucket" "storage_bucket" {
  project = var.project_id
  for_each      = toset(var.bucket_name_set)
  name          = each.value

  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"
  public_access_prevention = "enforced"

}

resource "google_bigquery_dataset" "default" {
  dataset_id                  = "avro_dataset_9494959"
  friendly_name               = "avro_dataset"
  description                 = "dataset used to store avro files"
  location                    = var.region
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "bar"

  time_partitioning {
    type = "DAY"
  }

  labels = {
    env = "default"
  }

}

resource "google_bigquery_table" "sheet" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "avro_sheet"
}