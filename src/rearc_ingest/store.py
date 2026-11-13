"""Apply D10's write order and D11's removal to the Volumes and the manifest.

Per changed file: archive copy (named by hash) -> raw copy -> manifest entry.
The manifest is saved after every file, so a crash leaves at most one file's
write unrecorded. The next run redoes that file with the same bytes.
"""
from __future__ import annotations

import hashlib
from collections.abc import Callable

from .config import Settings
from .manifest import Entry, Manifest
from .summary import RunSummary
from .volume import Volume


def content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class Store:
    def __init__(
        self,
        volume: Volume,
        settings: Settings,
        manifest: Manifest,
        summary: RunSummary,
        now: Callable[[], str],
    ) -> None:
        self.volume = volume
        self.settings = settings
        self.manifest = manifest
        self.summary = summary
        self._now = now

    def save_version(
        self,
        key: str,
        content: bytes,
        *,
        listing_timestamp: str | None = None,
        listing_size: str | None = None,
    ) -> None:
        sha256 = content_hash(content)
        known = self.manifest.files.get(key)

        if known is not None and known.status == "present" and known.sha256 == sha256:
            if (known.listing_timestamp, known.listing_size) == (listing_timestamp, listing_size):
                self.summary.unchanged.append(key)
                return
            # D7: same content, new listing metadata. Record it so later runs skip the
            # download. This is a manifest write, not a re-ingest.
            known.listing_timestamp = listing_timestamp
            known.listing_size = listing_size
            self._save_manifest()
            self.summary.touched.append(key)
            return

        raw_path = f"{self.settings.raw_root}/{key}"
        archive_path = f"{self.settings.archive_root}/{key}/{sha256}"
        self.volume.write_bytes(archive_path, content)
        self.summary.archive_writes += 1
        self.volume.write_bytes(raw_path, content)
        self.summary.raw_writes += 1
        self.manifest.files[key] = Entry(
            raw_path=raw_path,
            archive_path=archive_path,
            sha256=sha256,
            written_at=self._now(),
            listing_timestamp=listing_timestamp,
            listing_size=listing_size,
        )
        self.manifest.refresh_pending = True  # D19: saved together with the entry
        self._save_manifest()
        self.summary.written.append(key)

    def remove(self, key: str) -> None:
        """D11: delete the raw copy; the archive keeps every version written."""
        entry = self.manifest.files[key]
        self.volume.delete(entry.raw_path)
        self.summary.raw_deletes += 1
        entry.status = "removed"
        entry.removed_at = self._now()
        self.manifest.refresh_pending = True  # D19: saved together with the entry
        self._save_manifest()
        self.summary.removed.append(key)

    def mark_refresh_started(self) -> None:
        """D19: clear the flag once an update has successfully started."""
        self.manifest.refresh_pending = False
        self._save_manifest()

    def _save_manifest(self) -> None:
        self.volume.write_bytes(self.settings.manifest_path, self.manifest.to_json())
        self.summary.manifest_writes += 1
