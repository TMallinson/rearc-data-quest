"""The ingestion manifest: per-file state kept in the state Volume (D7, D8).

One entry per source file, holding:
- the listing metadata last seen (raw strings, BLS only);
- the hash of the content last written to raw, and where it was archived;
- whether the file is still published (present) or was removed (D11).
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Literal, Protocol

SCHEMA_VERSION = 1


class ManifestError(ValueError):
    """The stored manifest can't be read safely."""


class _Reader(Protocol):
    def read_bytes(self, path: str) -> bytes | None: ...


@dataclass
class Entry:
    raw_path: str
    archive_path: str
    sha256: str
    written_at: str  # ISO-8601 UTC time of the last raw write
    listing_timestamp: str | None = None  # BLS only; None for DataUSA
    listing_size: str | None = None
    status: Literal["present", "removed"] = "present"
    removed_at: str | None = None


@dataclass
class Manifest:
    files: dict[str, Entry] = field(default_factory=dict)
    # D19: raw changed since the last successfully started pipeline update.
    refresh_pending: bool = False

    def to_json(self) -> bytes:
        # Sorted, indented and deterministic, so the same state always gives the same bytes.
        document = {
            "schema_version": SCHEMA_VERSION,
            "refresh_pending": self.refresh_pending,
            "files": {key: asdict(entry) for key, entry in self.files.items()},
        }
        return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> Manifest:
        try:
            document = json.loads(data)
        except ValueError as error:
            raise ManifestError(f"manifest is not valid JSON: {error}") from error
        version = document.get("schema_version")
        if version != SCHEMA_VERSION:
            raise ManifestError(f"unsupported manifest schema_version: {version!r}")
        pending = document.get("refresh_pending")
        if not isinstance(pending, bool):
            raise ManifestError(f"manifest refresh_pending must be true or false, got {pending!r}")
        files = {key: Entry(**entry) for key, entry in document["files"].items()}
        return cls(files, refresh_pending=pending)


def load_manifest(volume: _Reader, path: str) -> Manifest:
    """A missing manifest means a first run: every listed file is new."""
    data = volume.read_bytes(path)
    return Manifest() if data is None else Manifest.from_json(data)
