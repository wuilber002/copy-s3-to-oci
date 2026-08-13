terraform {
  required_version = ">= 1.5.0, < 2.0.0"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 7.0"
    }
  }
}

# OCI Resource Manager injects its own authentication into the OCI provider.
# Do not set an auth method here; forcing ResourcePrincipal prevents ORM jobs
# from using their managed credentials.
provider "oci" {}
