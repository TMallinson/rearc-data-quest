import json

import pytest

from fakes import FakeVolume
from rearc_ingest.manifest import Entry, Manifest, ManifestError, load_manifest


def sample() -> Manifest:
    return Manifest({
        "bls/pr/pr.series": Entry("/Volumes/r", "/Volumes/a", "abc", "t", "11/5/2026  8:30 AM", "46068"),
        "datausa/population.json": Entry("/Volumes/r2", "/Volumes/a2", "def", "t"),
    }, refresh_pending=True)


def test_round_trip():
    assert Manifest.from_json(sample().to_json()) == sample()


def test_serialisation_is_deterministic():
    reordered = Manifest(dict(reversed(list(sample().files.items()))), refresh_pending=True)
    assert reordered.to_json() == sample().to_json()


def test_missing_or_invalid_refresh_pending_is_refused():
    with pytest.raises(ManifestError, match="refresh_pending"):
        Manifest.from_json(json.dumps({"schema_version": 1, "files": {}}).encode())


def test_missing_manifest_means_first_run():
    assert load_manifest(FakeVolume(), "/Volumes/x/manifest.json") == Manifest()


def test_unknown_schema_version_is_refused():
    with pytest.raises(ManifestError, match="schema_version"):
        Manifest.from_json(json.dumps({"schema_version": 99, "files": {}}).encode())


def test_invalid_json_is_refused():
    with pytest.raises(ManifestError, match="not valid JSON"):
        Manifest.from_json(b"{not json")
