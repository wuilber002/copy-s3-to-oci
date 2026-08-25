output "instance_id" {
  value       = oci_core_instance.migration.id
  description = "Migration VM OCID."
}

output "private_ip" {
  value       = oci_core_instance.migration.private_ip
  description = "Private IP of the migration VM."
}

output "vault_id" {
  value       = local.vault_id
  description = "Vault OCID."
}

output "vault_key_id" {
  value       = local.vault_key_id
  description = "Vault encryption key OCID."
}

output "secret_ocids" {
  value = merge(
    { postgres_password = var.create_platform_secrets ? oci_vault_secret.postgres_password[0].id : var.external_postgres_password_secret_ocid },
    { simulation_postgres_password = var.create_platform_secrets ? oci_vault_secret.simulation_postgres_password[0].id : var.external_simulation_postgres_password_secret_ocid },
    { initial_aws_connection = var.create_initial_aws_connection_secret ? oci_vault_secret.initial_aws_connection[0].id : null },
  )
  description = "Secret OCIDs. Real and simulation PostgreSQL passwords are generated separately by Terraform; AWS credentials belong only in customer-managed connection JSON Secrets."
}

output "dynamic_group_name" {
  value       = local.dynamic_group_name
  description = "Dynamic group used by the VM instance principal."
}

output "policy_statement_count" {
  value       = (var.create_oci_policy ? length(local.policy_statements) : 0) + (var.manage_secret_access_policy ? 2 * length(local.secret_compartment_ocids) : 0)
  description = "Total policy statements created: destination bucket access plus two Secret discovery/read statements per configured Secret compartment."
}

output "boot_volume_backup_policy_id" {
  value       = local.effective_backup_policy_id
  description = "Automatic boot-volume backup policy attached to the migration VM, if configured."
}
