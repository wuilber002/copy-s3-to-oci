# Cleanup of controlled test data

RAIJIN never deletes S3 source data or OCI destination data automatically. The
migration role intentionally has no delete permission. This protects production
data and makes cleanup an explicit operator action after a validation test.

Use `scripts/cleanup-test-objects.sh` only with an exact list of test keys. It
does not accept a prefix, a directory marker, or a wildcard. Its default is a
read-only plan; deletion requires both `--apply` and the literal confirmation
`--confirm DELETE_TEST_OBJECTS`.

Create `test-keys.txt` with one exact key per line, for example:

```text
raijin-validation/2026-08-17/large-object.bin
```

Review the plan first:

```bash
./scripts/cleanup-test-objects.sh \
  --keys-file test-keys.txt \
  --s3-bucket my-authorized-test-source \
  --oci-bucket my-authorized-test-destination \
  --oci-namespace <object-storage-namespace>
```

After confirming every key belongs to the controlled test, apply it:

```bash
./scripts/cleanup-test-objects.sh \
  --keys-file test-keys.txt \
  --s3-bucket my-authorized-test-source \
  --oci-bucket my-authorized-test-destination \
  --oci-namespace <object-storage-namespace> \
  --apply --confirm DELETE_TEST_OBJECTS
```

Use `--keep-source` to remove only OCI test copies. Do not remove a Glacier or
Deep Archive source object until its retention, legal-hold, and billing rules
have been reviewed. The operator identity that runs this script must have the
normal delete permissions on the named test buckets; the RAIJIN migration role
must remain read/write-only for migration, without delete permissions.

Finally archive or delete the test source in **Migrations** according to its
wave history, and retain the wave report and manifest as test evidence.
