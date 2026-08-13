variable "compartment_ocid" {
  description = "Compartment in which the migration VM is created."
  type        = string
}

variable "tenancy_ocid" {
  description = "OCI tenancy OCID. Resource Manager prepopulates this value."
  type        = string
}

variable "region" {
  description = "OCI region. Resource Manager prepopulates this value."
  type        = string
}

variable "availability_domain" {
  description = "Availability domain for the migration VM."
  type        = string
}

variable "subnet_ocid" {
  description = "Existing subnet with HTTPS egress to AWS and OCI APIs."
  type        = string
}

variable "image_ocid" {
  description = "Linux image OCID for the migration VM."
  type        = string
}

variable "instance_shape" {
  description = "Flexible x86 VM shape."
  type        = string
  default     = "VM.Standard.E5.Flex"
}

variable "ocpus" {
  description = "OCPUs for the migration VM. Recommended: 8."
  type        = number
  default     = 8

  validation {
    condition     = var.ocpus >= 4
    error_message = "Use at least 4 OCPUs; 8 OCPUs is recommended for a 1.2 Gbps migration link."
  }
}

variable "memory_in_gbs" {
  description = "Memory for the migration VM. Recommended: 32 GB."
  type        = number
  default     = 32

  validation {
    condition     = var.memory_in_gbs >= 16
    error_message = "Use at least 16 GB RAM; 32 GB is recommended for this workload."
  }
}

variable "boot_volume_size_in_gbs" {
  description = "Boot volume size. Holds PostgreSQL, local backups, logs, and application releases."
  type        = number
  default     = 500

  validation {
    condition     = var.boot_volume_size_in_gbs >= 200
    error_message = "Use at least 200 GB for the boot volume; 500 GB is recommended."
  }
}

variable "ssh_public_key" {
  description = "SSH public key used only for administration and localhost web-interface tunnels."
  type        = string
}

variable "assign_public_ip" {
  description = "Assign a public IPv4 address to the VM. Enable only when the selected subnet and security rules restrict SSH appropriately."
  type        = bool
  default     = false
}

variable "resource_name_prefix" {
  description = "Prefix used in names of all created resources."
  type        = string
  default     = "s3-oci-migration"

  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9-]{1,40}$", var.resource_name_prefix))
    error_message = "Use 2-41 letters, digits, or hyphens, beginning with a letter."
  }
}

variable "vault_compartment_ocid" {
  description = "Compartment where the Vault is created."
  type        = string
}

variable "key_compartment_ocid" {
  description = "Compartment where the Vault encryption key is created."
  type        = string
}

variable "secrets_compartment_ocid" {
  description = "Compartment where the Vault Secrets are created."
  type        = string
}

variable "create_dynamic_group" {
  description = "Whether Terraform creates the instance dynamic group."
  type        = bool
  default     = true
}

variable "dynamic_group_name" {
  description = "Name for the dynamic group when it is created by Terraform."
  type        = string
  default     = ""
}

variable "existing_dynamic_group_name" {
  description = "Existing dynamic group name when create_dynamic_group is false."
  type        = string
  default     = ""
}

variable "create_oci_policy" {
  description = "Whether Terraform creates the OCI policy granting the VM access to Vault and destination buckets."
  type        = bool
  default     = true
}

variable "policy_compartment_ocid" {
  description = "Compartment where the IAM policy is created. It must govern every selected destination bucket compartment."
  type        = string
  default     = ""
}

variable "destination_buckets_json" {
  description = "JSON array of OCI destinations: [{\"name\":\"bucket-a\",\"compartment_id\":\"ocid1.compartment...\"}]. Required when creating the policy."
  type        = string
  default     = "[]"
}

variable "backup_policy_id" {
  description = "Optional existing OCI boot-volume backup policy OCID. Leave blank to configure it manually after provisioning."
  type        = string
  default     = ""
}
