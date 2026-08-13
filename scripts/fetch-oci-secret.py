#!/usr/bin/env python3
"""Materialize one OCI Vault secret into a root-only local file.

The script authenticates exclusively with the VM instance principal. It never
prints secret content, which keeps systemd/cloud-init logs free of passwords.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import stat
import tempfile

import oci


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-config", required=True)
    parser.add_argument("--secret-name", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    with open(args.runtime_config, encoding="utf-8") as config_file:
        secret_ocid = json.load(config_file)["secret_ocids"][args.secret_name]

    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = oci.secrets.SecretsClient({}, signer=signer)
    bundle = client.get_secret_bundle(secret_ocid).data
    value = base64.b64decode(bundle.secret_bundle_content.content).decode("utf-8").strip()
    if not value or value.startswith("REPLACE_THIS_PLACEHOLDER"):
        raise RuntimeError(f"Secret {args.secret_name} is empty or still a placeholder")

    directory = os.path.dirname(args.output)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    descriptor, temporary_path = tempfile.mkstemp(prefix=".secret-", dir=directory)
    try:
        os.fchmod(descriptor, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output_file:
            output_file.write(value)
            output_file.write("\n")
        os.replace(temporary_path, args.output)
        os.chmod(args.output, stat.S_IRUSR | stat.S_IWUSR)
    finally:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)


if __name__ == "__main__":
    main()
