# AWS connections

An AWS connection is a reusable, immutable local label associated with one OCI
Vault Secret. Multiple S3 sources can use the same connection; sources in
different AWS accounts use different connections.

## Secret JSON schema v1

Create a Secret in a compartment that the RAIJIN VM can inspect and read. Its
current version must contain this JSON document:

```json
{
  "schema_version": 1,
  "connection_name": "Financeiro Produção",
  "aws_account_id": "123456789012",
  "default_region": "us-east-1",
  "bootstrap_access_key_id": "AKIA...",
  "bootstrap_secret_access_key": "...",
  "migration_role_arn": "arn:aws:iam::123456789012:role/s3-oci-migration-role",
  "batch_operations_role_arn": "arn:aws:iam::123456789012:role/s3-oci-batch-restore-role",
  "control_bucket": "financeiro-raijin-control"
}
```

`connection_name` is only a suggested label. When the connection is created,
RAIJIN copies the operator-confirmed label into PostgreSQL and never changes it
from a later Secret version. Internal processing uses database IDs, never this
label.

The access key is used only to assume `migration_role_arn`; temporary AWS
credentials are not persisted. The migration and Batch role ARNs must belong
to `aws_account_id`. Every connection requires a unique control bucket within
that AWS account. RAIJIN generates the manifest/report prefix from internal
connection, source and wave IDs.

## Operator flow

1. In **Settings → AWS connections**, click **Refresh OCI Secrets**.
2. RAIJIN reads every Secret it is permitted to inspect and read in the
   configured compartments. It caches only OCID, name and schema compatibility;
   it does not store or display Secret content.
3. Select a compatible Secret, set the immutable display label and register
   the connection.
4. Run its pre-check. It validates the payload, AssumeRole, account identity
   and control bucket without listing source objects or starting a restore.
5. In **Migrations**, select the connection when creating each source.

Only currently compatible Secrets appear in the registration combobox. A
connection already registered remains in the database if a later Secret version
is invalid; its pre-check and operations then report the incompatibility.

## Terraform options

The Resource Manager stack supports three operating modes:

- Create a Vault and Key, then create platform Secrets.
- Reuse a customer Vault and Key by OCID; Terraform can still create platform
  Secrets and the optional first AWS-connection template Secret.
- Disable platform Secret creation and supply an existing PostgreSQL password
  Secret. The customer is then responsible for those Secrets and policies.

When **Create Secret discovery/read policies** is enabled, Terraform creates
two statements in each configured Secret compartment for the VM Dynamic Group:
inspect Secret metadata and read Secret bundles. Disable it only when the
customer manages equivalent least-privilege policies independently.
