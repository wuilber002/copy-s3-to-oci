import base64
import hashlib
import os
from pathlib import Path


# The worker imports the application module. These values make that import
# deterministic in CI; these unit tests never open a database connection.
_password = Path("/tmp/raijin-test-password")
_password.write_text("test-password", encoding="utf-8")
os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("POSTGRES_PASSWORD_FILE", str(_password))
os.environ.setdefault("OCI_RUNTIME_CONFIG_FILE", "/tmp/raijin-test-oci-runtime.json")

from app.real_worker import expected_part_size, multipart_audit_matches, multipart_parts_on_oci, reusable_multipart_part


class Part:
    def __init__(self, number, etag, size):
        self.part_num = number
        self.etag = etag
        self.size = size


class Response:
    def __init__(self, parts, page=None):
        self.data = type("Data", (), {"parts": parts})()
        self.headers = {"opc-next-page": page} if page else {}


class ListResponse:
    def __init__(self, parts, page=None):
        self.data = parts
        self.headers = {"opc-next-page": page} if page else {}


class MultipartClient:
    def __init__(self):
        self.calls = []

    def list_multipart_upload_parts(self, namespace, bucket, key, upload_id, **kwargs):
        self.calls.append(kwargs)
        if not kwargs.get("page"):
            return Response([Part(1, "etag-1", 64)], "next")
        return Response([Part(2, "etag-2", 11)])


def test_expected_part_size_handles_final_short_part():
    assert expected_part_size(139, 1, 64) == 64
    assert expected_part_size(139, 2, 64) == 64
    assert expected_part_size(139, 3, 64) == 11
    assert expected_part_size(139, 4, 64) == 0


def test_multipart_parts_listing_paginates_and_keeps_evidence():
    client = MultipartClient()
    parts = multipart_parts_on_oci(client, "ns", "bucket", "key", "upload")
    assert parts == {1: {"etag": "etag-1", "size": 64}, 2: {"etag": "etag-2", "size": 11}}
    assert client.calls == [{"limit": 1000}, {"limit": 1000, "page": "next"}]


def test_multipart_parts_listing_accepts_the_oci_sdk_bare_list_shape():
    class Client:
        def list_multipart_upload_parts(self, *_args, **_kwargs):
            return ListResponse([Part(1, "etag-1", 64)])

    assert multipart_parts_on_oci(Client(), "ns", "bucket", "key", "upload") == {1: {"etag": "etag-1", "size": 64}}


def test_resume_skips_only_a_remote_part_with_persisted_sha_evidence():
    assert reusable_multipart_part({"etag": "etag", "size": 64}, {"sha256": "digest"}, 64)
    assert not reusable_multipart_part({"etag": "etag", "size": 63}, {"sha256": "digest"}, 64)
    assert not reusable_multipart_part({"etag": "etag", "size": 64}, {}, 64)


def test_resumed_multipart_deep_audit_uses_part_evidence_without_source_reread():
    digests = [hashlib.sha256(b"first").digest(), hashlib.sha256(b"second").digest()]
    evidence = {str(index): {"sha256": base64.b64encode(digest).decode()} for index, digest in enumerate(digests, 1)}
    assert multipart_audit_matches(evidence, digests)
    assert not multipart_audit_matches(evidence, [digests[1], digests[0]])
