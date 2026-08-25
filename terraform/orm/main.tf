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

  # Object access and bucket metadata inspection stay scoped to exactly the
  # compartments supplied in the destination-bucket form. This deliberately
  # minimizes OCI policy statements without using tenancy-wide permissions.
  object_storage_policy_statements = [
    for compartment_id, bucket_names in local.buckets_by_compartment : format(
      "Allow dynamic-group %s to manage objects in compartment id %s where any {%s}",
      local.dynamic_group_name,
      compartment_id,
      join(", ", [for bucket_name in bucket_names : "target.bucket.name='${bucket_name}'"])
    )
  ]

  bucket_inspection_policy_statements = [
    for compartment_id in local.bucket_compartments : format(
      "Allow dynamic-group %s to inspect buckets in compartment id %s",
      local.dynamic_group_name,
      compartment_id,
    )
  ]

  policy_statements = concat(local.bucket_inspection_policy_statements, local.object_storage_policy_statements)

  secret_compartment_ocids        = distinct(concat([var.secrets_compartment_ocid], jsondecode(var.secret_compartment_ocids_json)))
  vault_id                        = var.create_vault_key ? oci_kms_vault.migration[0].id : trimspace(var.existing_vault_ocid)
  vault_key_id                    = var.create_vault_key ? oci_kms_key.migration[0].id : trimspace(var.existing_vault_key_ocid)
  initial_secret_compartment_ocid = trimspace(var.initial_aws_connection_secret_compartment_ocid) != "" ? var.initial_aws_connection_secret_compartment_ocid : var.secrets_compartment_ocid

  effective_backup_policy_id = var.create_boot_volume_backup_policy ? oci_core_volume_backup_policy.migration[0].id : trimspace(var.backup_policy_id)

}

data "oci_objectstorage_namespace" "migration" {
  compartment_id = var.compartment_ocid
}

# Resolve names while the Resource Manager stack runs. The VM receives only a
# local OCID-to-name map, so the dynamic group needs no Identity permission
# just to render the destination-bucket selector.
data "oci_identity_compartment" "destination" {
  for_each = toset(local.bucket_compartments)
  id       = each.value
}

resource "random_password" "postgres" {
  length  = 48
  special = false
}

resource "random_password" "simulation_postgres" {
  length  = 48
  special = false
}

resource "oci_kms_vault" "migration" {
  count          = var.create_vault_key ? 1 : 0
  compartment_id = var.vault_compartment_ocid
  display_name   = "${var.resource_name_prefix}-vault"
  vault_type     = "DEFAULT"
}

resource "oci_kms_key" "migration" {
  count               = var.create_vault_key ? 1 : 0
  compartment_id      = var.key_compartment_ocid
  display_name        = "${var.resource_name_prefix}-key"
  management_endpoint = oci_kms_vault.migration[0].management_endpoint
  protection_mode     = "SOFTWARE"

  key_shape {
    algorithm = "AES"
    length    = 32
  }
}

resource "oci_vault_secret" "postgres_password" {
  count          = var.create_platform_secrets ? 1 : 0
  compartment_id = var.secrets_compartment_ocid
  secret_name    = "${var.resource_name_prefix}-postgres-password"
  vault_id       = local.vault_id
  key_id         = local.vault_key_id
  description    = "Automatically generated password for the local migration PostgreSQL user. Rotate only through the documented procedure."

  secret_content {
    content_type = "BASE64"
    content      = base64encode(random_password.postgres.result)
    name         = "terraform-generated"
    stage        = "CURRENT"
  }
}

resource "oci_vault_secret" "simulation_postgres_password" {
  count          = var.create_platform_secrets ? 1 : 0
  compartment_id = var.secrets_compartment_ocid
  secret_name    = "${var.resource_name_prefix}-simulation-postgres-password"
  vault_id       = local.vault_id
  key_id         = local.vault_key_id
  description    = "Automatically generated password for the isolated RAIJIN simulation PostgreSQL user."

  secret_content {
    content_type = "BASE64"
    content      = base64encode(random_password.simulation_postgres.result)
    name         = "terraform-generated"
    stage        = "CURRENT"
  }
}

resource "oci_vault_secret" "initial_aws_connection" {
  count          = var.create_initial_aws_connection_secret ? 1 : 0
  compartment_id = local.initial_secret_compartment_ocid
  secret_name    = var.initial_aws_connection_secret_name
  vault_id       = local.vault_id
  key_id         = local.vault_key_id
  description    = "RAIJIN AWS connection JSON template. Replace all placeholders before registering the connection in the web console."

  secret_content {
    content_type = "BASE64"
    content = base64encode(jsonencode({
      schema_version              = 1
      connection_name             = "REPLACE_WITH_IMMUTABLE_LABEL"
      aws_account_id              = "123456789012"
      default_region              = "us-east-1"
      bootstrap_access_key_id     = "REPLACE_WITH_ACCESS_KEY_ID"
      bootstrap_secret_access_key = "REPLACE_WITH_SECRET_ACCESS_KEY"
      migration_role_arn          = "arn:aws:iam::123456789012:role/s3-oci-migration-role"
      batch_operations_role_arn   = "arn:aws:iam::123456789012:role/s3-oci-batch-restore-role"
      control_bucket              = "REPLACE_WITH_UNIQUE_CONTROL_BUCKET"
    }))
    name  = "initial-template"
    stage = "CURRENT"
  }

  lifecycle { ignore_changes = [secret_content] }
}

moved {
  from = oci_kms_vault.migration
  to   = oci_kms_vault.migration[0]
}

moved {
  from = oci_kms_key.migration
  to   = oci_kms_key.migration[0]
}

moved {
  from = oci_vault_secret.postgres_password
  to   = oci_vault_secret.postgres_password[0]
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
        object_storage_namespace = data.oci_objectstorage_namespace.migration.namespace
        destination_compartment_names = {
          for compartment_id, compartment in data.oci_identity_compartment.destination : compartment_id => compartment.name
        }
        secret_ocids = {
          postgres_password            = var.create_platform_secrets ? oci_vault_secret.postgres_password[0].id : var.external_postgres_password_secret_ocid
          simulation_postgres_password = var.create_platform_secrets ? oci_vault_secret.simulation_postgres_password[0].id : var.external_simulation_postgres_password_secret_ocid
        }
        secrets_compartment_ocid = var.secrets_compartment_ocid
        secret_compartment_ocids = local.secret_compartment_ocids
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
    precondition {
      condition     = var.create_vault_key || (trimspace(var.existing_vault_ocid) != "" && trimspace(var.existing_vault_key_ocid) != "")
      error_message = "Provide existing_vault_ocid and existing_vault_key_ocid when create_vault_key is false."
    }
    precondition {
      condition     = var.create_platform_secrets || trimspace(var.external_postgres_password_secret_ocid) != ""
      error_message = "Provide external_postgres_password_secret_ocid when create_platform_secrets is false."
    }
    precondition {
      condition     = var.create_platform_secrets || trimspace(var.external_simulation_postgres_password_secret_ocid) != ""
      error_message = "Provide external_simulation_postgres_password_secret_ocid when create_platform_secrets is false."
    }
    precondition {
      condition     = !var.create_initial_aws_connection_secret || trimspace(var.initial_aws_connection_secret_name) != ""
      error_message = "Provide initial_aws_connection_secret_name when creating the initial AWS connection Secret."
    }
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

resource "oci_identity_policy" "secret_access" {
  for_each       = var.manage_secret_access_policy ? toset(local.secret_compartment_ocids) : toset([])
  compartment_id = each.value
  name           = "${var.resource_name_prefix}-secret-access-${substr(md5(each.value), 0, 8)}"
  description    = "Least-privilege Secret discovery and read access for ${var.resource_name_prefix} VM."
  statements = [
    "Allow dynamic-group ${local.dynamic_group_name} to inspect secret-family in compartment id ${each.value}",
    "Allow dynamic-group ${local.dynamic_group_name} to read secret-bundles in compartment id ${each.value}",
  ]
}

resource "oci_core_volume_backup_policy" "migration" {
  count          = var.create_boot_volume_backup_policy ? 1 : 0
  compartment_id = trimspace(var.backup_policy_compartment_ocid) != "" ? var.backup_policy_compartment_ocid : var.compartment_ocid
  display_name   = "${var.resource_name_prefix}-boot-volume-backup"

  schedules {
    backup_type       = "INCREMENTAL"
    period            = "ONE_DAY"
    retention_seconds = 3024000 # 35 days; covers the default simulator quarantine
    hour_of_day       = 2
    time_zone         = "UTC"
  }
}

resource "oci_core_volume_backup_policy_assignment" "boot_volume" {
  count     = var.create_boot_volume_backup_policy || trimspace(var.backup_policy_id) != "" ? 1 : 0
  asset_id  = oci_core_instance.migration.boot_volume_id
  policy_id = local.effective_backup_policy_id
}
