output "instance_id" {
  value       = oci_core_instance.migration.id
  description = "Migration VM OCID."
}

output "private_ip" {
  value       = oci_core_instance.migration.private_ip
  description = "Private IP of the migration VM."
}

output "vault_id" {
  value       = oci_kms_vault.migration.id
  description = "Vault OCID."
}

output "vault_key_id" {
  value       = oci_kms_key.migration.id
  description = "Vault encryption key OCID."
}

output "secret_ocids" {
  value       = { for name, secret in oci_vault_secret.migration : name => secret.id }
  description = "Secret OCIDs. Replace their placeholders in the OCI Console before use."
}

output "dynamic_group_name" {
  value       = local.dynamic_group_name
  description = "Dynamic group used by the VM instance principal."
}

output "policy_statement_count" {
  value       = var.create_oci_policy ? length(local.policy_statements) : 0
  description = "Policy statements created: two common statements plus one per destination-bucket compartment."
}
