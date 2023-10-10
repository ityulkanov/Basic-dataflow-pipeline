output "instance_id" {
  description = "The ID of the created instance."
  value       = google_compute_instance.default.id
}

output "instance_self_link" {
  description = "The self link of the created instance."
  value       = google_compute_instance.default.self_link
}

output "instance_public_ip" {
  description = "The public IP address assigned to the instance."
  value       = google_compute_instance.default.network_interface[0].access_config[0].nat_ip
}