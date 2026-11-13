"""Access to Unity Catalog Volumes, only through the Databricks SDK Files API (D1).

The Volume protocol is what the rest of the package depends on. Tests use an
in-memory fake (tests/fakes.py); real runs use SdkVolume.
"""
from __future__ import annotations

import io
from typing import Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


class Volume(Protocol):
    def read_bytes(self, path: str) -> bytes | None: ...

    def write_bytes(self, path: str, data: bytes) -> None: ...

    def delete(self, path: str) -> None: ...


class SdkVolume:
    def __init__(self, client: WorkspaceClient) -> None:
        self._files = client.files

    def read_bytes(self, path: str) -> bytes | None:
        _require_volume_path(path)
        try:
            return self._files.download(path).contents.read()
        except NotFound:
            return None

    def write_bytes(self, path: str, data: bytes) -> None:
        _require_volume_path(path)
        self._files.upload(path, io.BytesIO(data), overwrite=True)

    def delete(self, path: str) -> None:
        _require_volume_path(path)
        try:
            self._files.delete(path)
        except NotFound:
            pass  # already gone: deleting is idempotent, so a rerun after a crash is safe


def _require_volume_path(path: str) -> None:
    """Guard against local paths: writes must go to the Volume (D1)."""
    if not path.startswith("/Volumes/"):
        raise ValueError(f"not a Unity Catalog Volume path: {path!r}")
