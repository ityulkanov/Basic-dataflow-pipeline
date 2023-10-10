variable "credentials_path" {
  description = "The path to the GCP service account JSON key."
  type        = string
  default     = ""  # You can set a default or leave it empty to ensure it's provided.
}
variable "image_name" {
  description = "The name of the image to create."
  type        = string
  default     = "debian-11-bullseye-v20231004"
}