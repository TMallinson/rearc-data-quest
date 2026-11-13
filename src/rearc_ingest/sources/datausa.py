"""DataUSA population: no listing to compare, so always fetch and hash (D7).

D18 test (2026-11-13): two fetches returned identical bytes, so the raw bytes are
hashed and written as received. No normalisation.
"""
from __future__ import annotations

import json

from ..config import DATAUSA_KEY
from ..http import Fetcher
from ..store import Store


def ingest_datausa(fetcher: Fetcher, store: Store, *, dry_run: bool) -> None:
    if dry_run:
        store.summary.planned_downloads.append(f"{DATAUSA_KEY} (always fetched)")
        return
    content = fetcher.get(store.settings.datausa_url, validate=_looks_like_population)
    store.summary.datausa_fetches += 1
    store.save_version(DATAUSA_KEY, content)


def _looks_like_population(content: bytes) -> str | None:
    """Refuse to land an error page or empty payload as if it were the dataset."""
    try:
        document = json.loads(content)
    except ValueError as error:
        return f"response is not JSON ({error})"
    if not isinstance(document, dict) or not isinstance(document.get("data"), list):
        return "response has no 'data' list"
    if not document["data"]:
        return "response 'data' list is empty"
    return None
