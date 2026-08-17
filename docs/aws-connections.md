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

The **View configuration** action shows only non-sensitive fields from the
current Secret version: account, default region, control bucket and the two
role ARNs. The bootstrap access key and secret access key are never returned
to the browser. Use **Sync Secret** after rotating or correcting a Secret
version: it updates the connection's displayed region and control bucket while
preserving the immutable local label and AWS account identity.

Sources can belong to different AWS regions within the same account
connection. Before discovery and before every paid Batch restore submission,
RAIJIN checks the region returned by `HeadBucket` for both the source and the
control bucket. A restore is blocked unless the configured source region, the
actual source-bucket region and the control-bucket region are identical. The
operator can use **Sync AWS region** on a source to correct its local region
from AWS without repeating discovery; any pending restore task is superseded
so that the operator explicitly creates a new safe attempt.

## Restore evidence and failure handling

Each Batch restore submission creates an immutable local restore attempt with
the AWS Batch Job ID, region, manifest ETag, expected object count and later
the completion-report location and ETag. A wave does **not** move to
`RESTORING` merely because the Batch Job is `Complete`. RAIJIN requires that
the job counters match the manifest, every task succeeds and the completion
report is imported per object before it records `RESTORE_REQUEST_ACCEPTED`.

For each report row, RAIJIN preserves the object/version, task result, HTTP
status, AWS error code and error message. If the job has failures, the wave is
placed in `RESTORE_REQUEST_FAILED`; transfer and availability polling stop.
Some all-failed jobs do not produce a completion report, so RAIJIN persists
the Batch Job counters and explicitly records the missing evidence rather than
claiming that a restore is in progress. Reprocessing retains the previous
attempt for audit and creates a new Batch Job only after the operator has
corrected the cause.

Only currently compatible Secrets appear in the registration combobox. A
connection already registered remains in the database if a later Secret version
is invalid; its pre-check and operations then report the incompatibility.

## Retiring a legacy installation

Older RAIJIN releases used global AWS fields and separate credential Secrets.
Before upgrading, register and pre-check an equivalent JSON connection. For
each active source, use the audited connection-adoption operation; it verifies
the AWS region, migration role, Batch Operations role and control bucket before
changing only the source-to-connection reference. Inventory, waves, queue and
events are retained unchanged. Once every active source has a connection,
RAIJIN clears the global AWS configuration and no worker fallback remains.

Terraform then removes only the legacy access-key and secret-key Secrets it
previously managed. Customer-created legacy Secrets should be scheduled for
deletion through the OCI Console after the connection pre-check and a normal
worker cycle have succeeded.

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
