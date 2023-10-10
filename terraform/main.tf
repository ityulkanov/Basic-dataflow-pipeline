provider "google" {
  credentials = file(var.credentials_path)
  project     = "transformjson-401609"
  region      = "us-east1"
}

resource "google_compute_instance" "default" {
  name         = "terraform-instance"
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