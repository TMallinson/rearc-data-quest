"""One ingestion run: BLS, then DataUSA, then a pipeline update if raw has changed
since the last successfully started update (D19)."""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from .config import Settings
from .http import Fetcher
from .manifest import load_manifest
from .refresh import PipelineTrigger
from .sources.bls import ingest_bls
from .sources.datausa import ingest_datausa
from .store import Store
from .summary import RunSummary
from .volume import Volume


class RefreshError(RuntimeError):
    """Raw is ingested, but the pipeline update couldn't be started. The flag stays set."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run(
    settings: Settings,
    summary: RunSummary,
    *,
    bls_fetcher: Fetcher,
    datausa_fetcher: Fetcher,
    volume: Volume,
    trigger: PipelineTrigger | None,
    allow_removals: bool = False,
    dry_run: bool = False,
    refresh: bool = True,
    now: Callable[[], str] = utc_now,
) -> RunSummary:
    """Fills `summary` as it goes, so the caller can report progress even if a step raises."""
    summary.dry_run = dry_run
    if refresh and not dry_run and trigger is None:
        raise ValueError("refresh requested but no pipeline trigger is configured")

    store = Store(volume, settings, load_manifest(volume, settings.manifest_path), summary, now)
    ingest_bls(bls_fetcher, store, allow_removals=allow_removals, dry_run=dry_run)
    ingest_datausa(datausa_fetcher, store, dry_run=dry_run)

    # Reached only if every step succeeded, so the pipeline never sees a half-finished run.
    # The flag also covers raw changes from earlier failed or --no-refresh runs.
    if refresh and not dry_run and store.manifest.refresh_pending:
        try:
            update_id = trigger.trigger()
        except Exception as error:  # any failure to start leaves the flag set for the next run
            summary.refresh_pending = True
            raise RefreshError(
                f"raw is ingested, but the pipeline update could not be started ({error}). "
                "The change stays marked as pending; the next run will try again."
            ) from error
        summary.pipeline_update_id = update_id
        summary.refresh_triggered = True
        store.mark_refresh_started()
    summary.refresh_pending = store.manifest.refresh_pending
    return summary
