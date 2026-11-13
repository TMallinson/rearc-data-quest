"""Decide what a BLS run must do, before anything is downloaded or written.

Pure: compares the listing with the manifest (D7) and applies the removal guard (D11).
Because the guard runs here, a refused removal stops the run before any write.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatchcase

from .config import BLS_PREFIX
from .listing import ListingEntry
from .manifest import Manifest


class RemovalGuardError(RuntimeError):
    """Removals beyond the guard need --allow-removals (D11)."""


class Reason(str, Enum):
    NEW = "new"  # never seen before
    CHANGED = "changed"  # listing timestamp or size differs from the manifest
    REAPPEARED = "reappeared"  # removed earlier, listed again


@dataclass(frozen=True)
class Download:
    key: str
    entry: ListingEntry
    reason: Reason


@dataclass(frozen=True)
class Removal:
    key: str
    name: str


@dataclass(frozen=True)
class Plan:
    downloads: tuple[Download, ...]
    unchanged: tuple[str, ...]
    removals: tuple[Removal, ...]


def bls_key(name: str) -> str:
    return BLS_PREFIX + name


def plan_bls(
    entries: tuple[ListingEntry, ...],
    manifest: Manifest,
    *,
    max_removals: int,
    protected_glob: str,
    allow_removals: bool,
) -> Plan:
    downloads: list[Download] = []
    unchanged: list[str] = []
    listed_keys: set[str] = set()

    for entry in entries:
        key = bls_key(entry.name)
        listed_keys.add(key)
        known = manifest.files.get(key)
        if known is None:
            downloads.append(Download(key, entry, Reason.NEW))
        elif known.status == "removed":
            downloads.append(Download(key, entry, Reason.REAPPEARED))
        elif (known.listing_timestamp, known.listing_size) != (entry.timestamp_raw, entry.size_raw):
            downloads.append(Download(key, entry, Reason.CHANGED))
        else:
            unchanged.append(key)

    removals = [
        Removal(key, key[len(BLS_PREFIX):])
        for key, known in sorted(manifest.files.items())
        if key.startswith(BLS_PREFIX) and known.status == "present" and key not in listed_keys
    ]
    if removals and not allow_removals:
        _check_removal_guard(removals, max_removals, protected_glob)

    return Plan(tuple(downloads), tuple(unchanged), tuple(removals))


def _check_removal_guard(removals: list[Removal], max_removals: int, protected_glob: str) -> None:
    names = [removal.name for removal in removals]
    protected = [name for name in names if fnmatchcase(name, protected_glob)]
    reasons = []
    if len(names) > max_removals:
        reasons.append(f"{len(names)} removals exceeds the limit of {max_removals}")
    if protected:
        reasons.append(f"protected files matching {protected_glob!r}: {protected}")
    if reasons:
        raise RemovalGuardError(
            f"Refusing to remove {names} ({'; '.join(reasons)}). "
            "Nothing was changed. Check the source, then re-run with --allow-removals."
        )
