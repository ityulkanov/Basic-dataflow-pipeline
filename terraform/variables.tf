variable "credentials_path" {
  description = "The path to the GCP service account JSON key."
  type        = string
  default     = ""  # You can set a default or leave it empty to ensure it's provided.
}

variable "region" {
  description = "The region to create the image in."
  type        = string
  default     = "us-east1"
}

variable "bucket_name_set" {
  description = "A set of GCS bucket names..."
  type        = list(string)
}


variable "image_name" {
  description = "The name of the image to create."
  type        = string
  default     = "debian-11-bullseye-v20231004"
}

variable "project_id" {
  description = "The ID of the GCP project to create the image in."
  type        = string
  default     = "transformjson-401609"
}

variable "dataset_id" {
    description = "The ID of the BigQuery dataset to create the table in."
    type        = string
    default     = "avro_dataset_9494959"
}

variable "table_id" {
    description = "The ID of the BigQuery table to create."
    type        = string
    default     = "avro_data_table_9494959"
}