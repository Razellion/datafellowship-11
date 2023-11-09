provider "google" {
  credentials = var.credentials
  project     = var.project_id
  region      = "us-central1"
}

resource "google_compute_instance" "default" {
  name         = var.instance_name
  machine_type = var.instance_type
  zone         = "us-central1-b"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }
}

resource "google_storage_bucket" "default" {
  name     = var.bucket_id
  location = "us-central1"
}

resource "google_bigquery_dataset" "default" {
  dataset_id = var.dataset_id
  project    = var.project_id
  storage_billing_model = "LOGICAL"
  max_time_travel_hours = 168
}
