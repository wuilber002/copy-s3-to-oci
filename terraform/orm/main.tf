locals {
  requested_dynamic_group_name = trimspace(var.dynamic_group_name) != "" ? trimspace(var.dynamic_group_name) : "${var.resource_name_prefix}-vm"

  dynamic_group_name = var.create_dynamic_group ? local.requested_dynamic_group_name : trimspace(var.existing_dynamic_group_name)

  destination_buckets = jsondecode(var.destination_buckets_json)
  bucket_compartments = distinct([for bucket in local.destination_buckets : bucket.compartment_id])
  buckets_by_compartment = {
    for compartment_id in local.bucket_compartments : compartment_id => sort([
      for bucket in local.destination_buckets : bucket.name if bucket.compartment_id == compartment_id
    ])
  }

  # One object-storage statement per distinct bucket compartment, plus the two
  # common statements below. This deliberately minimizes OCI policy statements.
  object_storage_policy_statements = [
    for compartment_id, bucket_names in local.buckets_by_compartment : format(
      "Allow dynamic-group %s to manage objects in compartment id %s where any {%s}",
      local.dynamic_group_name,
      compartment_id,
      join(", ", [for bucket_name in bucket_names : "target.bucket.name='${bucket_name}'"])
    )
  ]

  common_policy_statements = [
    "Allow dynamic-group ${local.dynamic_group_name} to read secret-bundles in compartment id ${var.secrets_compartment_ocid}",
    "Allow dynamic-group ${local.dynamic_group_name} to read objectstorage-namespaces in tenancy"
  ]

  policy_statements = concat(local.common_policy_statements, local.object_storage_policy_statements)

  effective_backup_policy_id = var.create_boot_volume_backup_policy ? oci_core_volume_backup_policy.migration[0].id : trimspace(var.backup_policy_id)

  secret_placeholders = {
    aws_access_key_id     = <<-EOT
      REPLACE_THIS_PLACEHOLDER.
      Enter the AWS Access Key ID for the bootstrap IAM principal.
      This principal must be permitted only to call sts:AssumeRole on the migration role configured in the web interface.
      See docs/aws-setup.md before replacing this value.
    EOT
    aws_secret_access_key = <<-EOT
      REPLACE_THIS_PLACEHOLDER.
      Enter the AWS Secret Access Key paired with aws-access-key-id.
      Do not enter a session token. This credential is used only to assume the migration role.
      See docs/aws-setup.md before replacing this value.
    EOT
  }
}

resource "random_password" "postgres" {
  length  = 48
  special = false
}

resource "oci_kms_vault" "migration" {
  compartment_id = var.vault_compartment_ocid
  display_name   = "${var.resource_name_prefix}-vault"
  vault_type     = "DEFAULT"
}

resource "oci_kms_key" "migration" {
  compartment_id      = var.key_compartment_ocid
  display_name        = "${var.resource_name_prefix}-key"
  management_endpoint = oci_kms_vault.migration.management_endpoint
  protection_mode     = "SOFTWARE"

  key_shape {
    algorithm = "AES"
    length    = 32
  }
}

resource "oci_vault_secret" "aws" {
  for_each = local.secret_placeholders

  compartment_id = var.secrets_compartment_ocid
  secret_name    = "${var.resource_name_prefix}-${replace(each.key, "_", "-")}"
  vault_id       = oci_kms_vault.migration.id
  key_id         = oci_kms_key.migration.id
  description    = "Initial placeholder only. Replace this secret version with the customer value before operating the migration."

  secret_content {
    content_type = "BASE64"
    content      = base64encode(trimspace(each.value))
    name         = "initial-placeholder"
    stage        = "CURRENT"
  }

  # The initial version is only an instruction. The operator replaces it in
  # Vault; subsequent Terraform runs must never rotate it back to a placeholder.
  lifecycle {
    ignore_changes = [secret_content]
  }
}

resource "oci_vault_secret" "postgres_password" {
  compartment_id = var.secrets_compartment_ocid
  secret_name    = "${var.resource_name_prefix}-postgres-password"
  vault_id       = oci_kms_vault.migration.id
  key_id         = oci_kms_key.migration.id
  description    = "Automatically generated password for the local migration PostgreSQL user. Rotate only through the documented procedure."

  secret_content {
    content_type = "BASE64"
    content      = base64encode(random_password.postgres.result)
    name         = "terraform-generated"
    stage        = "CURRENT"
  }
}

moved {
  from = oci_vault_secret.migration["aws_access_key_id"]
  to   = oci_vault_secret.aws["aws_access_key_id"]
}

moved {
  from = oci_vault_secret.migration["aws_secret_access_key"]
  to   = oci_vault_secret.aws["aws_secret_access_key"]
}

moved {
  from = oci_vault_secret.migration["postgres_password"]
  to   = oci_vault_secret.postgres_password
}

resource "oci_core_instance" "migration" {
  availability_domain = var.availability_domain
  compartment_id      = var.compartment_ocid
  display_name        = "${var.resource_name_prefix}-vm"
  shape               = var.instance_shape

  shape_config {
    ocpus         = var.ocpus
    memory_in_gbs = var.memory_in_gbs
  }

  create_vnic_details {
    subnet_id        = var.subnet_ocid
    assign_public_ip = var.assign_public_ip
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
    user_data = base64encode(templatefile("${path.module}/cloud-init.yaml.tftpl", {
      bootstrap_repository = var.bootstrap_repository
      bootstrap_ref        = var.bootstrap_ref
      oci_runtime_config = jsonencode({
        secret_ocids = merge(
          { for name, secret in oci_vault_secret.aws : name => secret.id },
          { postgres_password = oci_vault_secret.postgres_password.id },
        )
      })
    }))
  }

  source_details {
    source_type                     = "image"
    source_id                       = var.image_ocid
    boot_volume_size_in_gbs         = var.boot_volume_size_in_gbs
    is_preserve_boot_volume_enabled = true
  }

  lifecycle {
    # cloud-init is first-boot configuration. Updating release metadata must
    # never replace a persistent migration VM.
    ignore_changes = [metadata["user_data"]]
  }
}

resource "oci_identity_dynamic_group" "migration_vm" {
  count          = var.create_dynamic_group ? 1 : 0
  compartment_id = var.tenancy_ocid
  name           = local.dynamic_group_name
  description    = "Instance principal for ${var.resource_name_prefix} migration VM."
  matching_rule  = "ALL {instance.id = '${oci_core_instance.migration.id}'}"
}

resource "oci_identity_policy" "migration_vm" {
  count          = var.create_oci_policy ? 1 : 0
  compartment_id = var.policy_compartment_ocid
  name           = "${var.resource_name_prefix}-vm-policy"
  description    = "Least-privilege access for ${var.resource_name_prefix} migration VM."
  statements     = local.policy_statements

  lifecycle {
    precondition {
      condition     = local.dynamic_group_name != ""
      error_message = "Provide existing_dynamic_group_name when create_dynamic_group is false."
    }
    precondition {
      condition     = length(local.destination_buckets) > 0
      error_message = "Provide at least one destination bucket when create_oci_policy is true."
    }
    precondition {
      condition     = trimspace(var.policy_compartment_ocid) != ""
      error_message = "Provide policy_compartment_ocid when create_oci_policy is true."
    }
  }
}

resource "oci_core_volume_backup_policy" "migration" {
  count          = var.create_boot_volume_backup_policy ? 1 : 0
  compartment_id = trimspace(var.backup_policy_compartment_ocid) != "" ? var.backup_policy_compartment_ocid : var.compartment_ocid
  display_name   = "${var.resource_name_prefix}-boot-volume-backup"

  schedules {
    backup_type       = "INCREMENTAL"
    period            = "ONE_DAY"
    retention_seconds = 1209600 # 14 days
    hour_of_day       = 2
    time_zone         = "UTC"
  }
}

resource "oci_core_volume_backup_policy_assignment" "boot_volume" {
  count     = var.create_boot_volume_backup_policy || trimspace(var.backup_policy_id) != "" ? 1 : 0
  asset_id  = oci_core_instance.migration.boot_volume_id
  policy_id = local.effective_backup_policy_id
}
