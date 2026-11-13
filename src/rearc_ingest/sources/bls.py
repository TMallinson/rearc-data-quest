"""BLS pr folder: discover files from the listing, download changed ones, mirror removals."""
from __future__ import annotations

from ..http import FetchError, Fetcher, InvalidBodyError, Validator
from ..listing import parse_listing
from ..plan import Plan, plan_bls
from ..store import Store


class SizeMismatchError(FetchError):
    """The downloaded size never matched the listing (D21). Nothing was written for the file."""


def ingest_bls(fetcher: Fetcher, store: Store, *, allow_removals: bool, dry_run: bool) -> Plan:
    settings = store.settings
    html = fetcher.get(settings.bls_base_url).decode("utf-8")
    listing = parse_listing(html, settings.bls_base_url)
    store.summary.listed_files = len(listing.entries)
    store.summary.skipped_dirs = list(listing.skipped_dirs)

    # The plan, including the removal guard, is complete before anything is written.
    plan = plan_bls(
        listing.entries,
        store.manifest,
        max_removals=settings.max_removals_without_flag,
        protected_glob=settings.protected_removal_glob,
        allow_removals=allow_removals,
    )
    if dry_run:
        store.summary.planned_downloads = [f"{d.key} ({d.reason.value})" for d in plan.downloads]
        store.summary.planned_removals = [r.key for r in plan.removals]
        return plan

    for download in plan.downloads:
        entry = download.entry
        try:
            content = fetcher.get(entry.url, validate=_expect_size(entry.size_raw))
        except InvalidBodyError as error:
            raise SizeMismatchError(
                f"{entry.name}: {error}. The file may have changed on the server during "
                "this run; re-run ingestion."
            ) from error
        store.summary.bls_downloads += 1
        store.save_version(
            download.key,
            content,
            listing_timestamp=entry.timestamp_raw,
            listing_size=entry.size_raw,
        )
    for removal in plan.removals:
        store.remove(removal.key)
    store.summary.unchanged.extend(plan.unchanged)
    return plan


def _expect_size(size_raw: str) -> Validator:
    """D21: the listing size is the uncompressed byte count. Any difference, shorter or
    longer, means the content and the listing don't describe the same version."""
    expected = int(size_raw)

    def check(content: bytes) -> str | None:
        if len(content) != expected:
            return f"received {len(content)} bytes but the listing says {expected}"
        return None

    return check
