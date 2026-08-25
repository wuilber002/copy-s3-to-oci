"""Deterministic, random-access payload generation for DATA simulations."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
from typing import Iterable, Iterator


GENERATOR_VERSION = "RAIJIN-DATA-V1"
GENERATOR_BLOCK_BYTES = 1024 * 1024
DEFAULT_STREAM_CHUNK_BYTES = 1024 * 1024


class SimulatedDataIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class VirtualContentDescriptor:
    scenario_seed: str
    object_id: str
    object_key: str
    object_version: str
    size_bytes: int
    generator_version: str = GENERATOR_VERSION

    def validate(self) -> None:
        if self.generator_version != GENERATOR_VERSION:
            raise ValueError(f"Unsupported simulated data generator {self.generator_version}")
        if self.size_bytes < 0:
            raise ValueError("Virtual object size cannot be negative")


def _generator_identity(descriptor: VirtualContentDescriptor) -> bytes:
    descriptor.validate()
    fields = (
        descriptor.generator_version,
        descriptor.scenario_seed,
        descriptor.object_id,
        descriptor.object_key,
        descriptor.object_version,
    )
    encoded = [field.encode("utf-8") for field in fields]
    return b"RAIJIN\0" + b"".join(len(field).to_bytes(4, "big") + field for field in encoded)


def deterministic_block(descriptor: VirtualContentDescriptor, block_index: int) -> bytes:
    if block_index < 0:
        raise ValueError("Block index cannot be negative")
    material = _generator_identity(descriptor) + block_index.to_bytes(8, "big")
    return hashlib.shake_256(material).digest(GENERATOR_BLOCK_BYTES)


def iter_deterministic_range(
    descriptor: VirtualContentDescriptor,
    offset: int = 0,
    length: int | None = None,
    chunk_size: int = DEFAULT_STREAM_CHUNK_BYTES,
) -> Iterator[bytes]:
    """Generate exactly one range while retaining at most one block in memory."""
    descriptor.validate()
    if offset < 0 or offset > descriptor.size_bytes:
        raise ValueError("Range offset is outside the virtual object")
    available = descriptor.size_bytes - offset
    requested = available if length is None else length
    if requested < 0 or requested > available:
        raise ValueError("Range length is outside the virtual object")
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")

    position = offset
    remaining = requested
    pending = bytearray()
    while remaining:
        block_index, in_block = divmod(position, GENERATOR_BLOCK_BYTES)
        block = deterministic_block(descriptor, block_index)
        take = min(remaining, GENERATOR_BLOCK_BYTES - in_block)
        pending.extend(block[in_block:in_block + take])
        position += take
        remaining -= take
        while len(pending) >= chunk_size:
            yield bytes(pending[:chunk_size])
            del pending[:chunk_size]
    if pending:
        yield bytes(pending)


@dataclass(frozen=True)
class StreamEvidence:
    size_bytes: int
    checksum_sha256: str


def consume_and_discard(
    chunks: Iterable[bytes],
    *,
    expected_size: int | None = None,
    expected_checksum_sha256: str | None = None,
) -> StreamEvidence:
    """Hash bytes actually received and immediately release every chunk."""
    digest = hashlib.sha256()
    size = 0
    for chunk in chunks:
        if not isinstance(chunk, bytes):
            chunk = bytes(chunk)
        digest.update(chunk)
        size += len(chunk)
    checksum = base64.b64encode(digest.digest()).decode("ascii")
    if expected_size is not None and size != expected_size:
        raise SimulatedDataIntegrityError(
            f"Received {size} bytes, expected {expected_size}"
        )
    if expected_checksum_sha256 is not None and checksum != expected_checksum_sha256:
        raise SimulatedDataIntegrityError("Received SHA-256 does not match expected evidence")
    return StreamEvidence(size_bytes=size, checksum_sha256=checksum)


def deterministic_sha256(descriptor: VirtualContentDescriptor) -> str:
    return consume_and_discard(iter_deterministic_range(descriptor)).checksum_sha256
