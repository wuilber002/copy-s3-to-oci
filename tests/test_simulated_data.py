import base64
import hashlib

import pytest

from app.simulated_data import (
    GENERATOR_BLOCK_BYTES,
    SimulatedDataIntegrityError,
    VirtualContentDescriptor,
    consume_and_discard,
    deterministic_sha256,
    iter_deterministic_range,
)


def descriptor(**overrides):
    values = {
        "scenario_seed": "scenario-42",
        "object_id": "object-7",
        "object_key": "prefix/file.bin",
        "object_version": "v1",
        "size_bytes": GENERATOR_BLOCK_BYTES * 2 + 317,
    }
    values.update(overrides)
    return VirtualContentDescriptor(**values)


def test_ranges_are_identical_to_slices_of_full_content():
    item = descriptor()
    full = b"".join(iter_deterministic_range(item, chunk_size=131_071))

    offset = GENERATOR_BLOCK_BYTES - 97
    length = GENERATOR_BLOCK_BYTES + 211
    selected = b"".join(
        iter_deterministic_range(item, offset=offset, length=length, chunk_size=65_537)
    )

    assert len(full) == item.size_bytes
    assert selected == full[offset:offset + length]


def test_seed_and_identity_define_stable_content():
    original = b"".join(iter_deterministic_range(descriptor(size_bytes=4096)))
    replay = b"".join(iter_deterministic_range(descriptor(size_bytes=4096)))
    changed = b"".join(
        iter_deterministic_range(descriptor(size_bytes=4096, scenario_seed="scenario-43"))
    )

    assert replay == original
    assert changed != original


def test_destination_evidence_hashes_bytes_actually_received():
    chunks = [b"raijin", b"-", b"simulation"]
    expected = base64.b64encode(hashlib.sha256(b"".join(chunks)).digest()).decode("ascii")

    evidence = consume_and_discard(
        chunks,
        expected_size=len(b"".join(chunks)),
        expected_checksum_sha256=expected,
    )

    assert evidence.size_bytes == 17
    assert evidence.checksum_sha256 == expected


def test_destination_rejects_size_or_checksum_divergence():
    with pytest.raises(SimulatedDataIntegrityError, match="expected 4"):
        consume_and_discard([b"abc"], expected_size=4)

    with pytest.raises(SimulatedDataIntegrityError, match="SHA-256"):
        consume_and_discard([b"abc"], expected_checksum_sha256="invalid")


def test_full_checksum_is_independent_of_stream_chunking():
    item = descriptor(size_bytes=GENERATOR_BLOCK_BYTES + 123)
    expected = deterministic_sha256(item)
    observed = consume_and_discard(
        iter_deterministic_range(item, chunk_size=7777)
    ).checksum_sha256

    assert observed == expected


@pytest.mark.parametrize(
    ("offset", "length"),
    [(-1, 1), (10_000, 1), (0, -1), (3000, 2000)],
)
def test_invalid_ranges_are_rejected(offset, length):
    with pytest.raises(ValueError):
        list(iter_deterministic_range(descriptor(size_bytes=4096), offset, length))
