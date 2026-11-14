"""D27: automated mitigation for databricks/cli#4872.

On CLI v1.22.1, removing a resource from the bundle config deletes it on the next
deploy even if it had lifecycle.prevent_destroy. The archive Volume (which can't be
rebuilt) and the schema (whose deletion may take its Volumes with it) must therefore
never disappear from resources/ or lose the flag. This test fails if they do.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]


def _load_resources(root: Path) -> dict:
    bundle = yaml.safe_load((root / "databricks.yml").read_text())
    merged: dict = {}
    for pattern in bundle.get("include", []):
        for path in sorted(root.glob(pattern)):
            for kind, items in (yaml.safe_load(path.read_text()) or {}).get("resources", {}).items():
                merged.setdefault(kind, {}).update(items or {})
    return merged


def _protected(resource: dict) -> bool:
    return (resource.get("lifecycle") or {}).get("prevent_destroy") is True


def protection_problems(root: Path) -> list[str]:
    resources = _load_resources(root)
    problems = []

    schemas = {r.get("name"): (key, r) for key, r in resources.get("schemas", {}).items()}
    if "${var.schema}" not in schemas:
        problems.append("the rearc_quest schema resource is missing from resources/")
    elif not _protected(schemas["${var.schema}"][1]):
        problems.append("the schema resource lacks lifecycle.prevent_destroy: true")

    archives = [r for r in resources.get("volumes", {}).values() if r.get("name") == "archive"]
    if not archives:
        problems.append("the archive Volume resource is missing from resources/")
    elif not _protected(archives[0]):
        problems.append("the archive Volume lacks lifecycle.prevent_destroy: true")
    return problems


def test_archive_volume_and_schema_are_present_and_protected():
    assert protection_problems(REPO) == []


def _copy_bundle(tmp_path: Path) -> Path:
    shutil.copy(REPO / "databricks.yml", tmp_path / "databricks.yml")
    shutil.copytree(REPO / "resources", tmp_path / "resources")
    return tmp_path


def test_check_catches_a_removed_flag(tmp_path):
    root = _copy_bundle(tmp_path)
    path = root / "resources" / "rearc_quest.volumes.yml"
    doc = yaml.safe_load(path.read_text())
    del doc["resources"]["volumes"]["archive"]["lifecycle"]
    path.write_text(yaml.safe_dump(doc))
    assert protection_problems(root) == ["the archive Volume lacks lifecycle.prevent_destroy: true"]


def test_check_catches_a_removed_resource(tmp_path):
    root = _copy_bundle(tmp_path)
    (root / "resources" / "rearc_quest.schema.yml").unlink()
    assert "the rearc_quest schema resource is missing from resources/" in protection_problems(root)


def test_ingestion_settings_match_the_bundle():
    """rearc-ingest and the bundle must agree, or the refresh trigger or Volume paths break."""
    from rearc_ingest.config import Settings

    defaults = Settings(bls_user_agent="x", datausa_user_agent="x")
    bundle = yaml.safe_load((REPO / "databricks.yml").read_text())
    variables = {name: spec["default"] for name, spec in bundle["variables"].items() if "default" in spec}
    assert (defaults.catalog, defaults.schema) == (variables["catalog"], variables["schema"])

    resources = _load_resources(REPO)
    (pipeline,) = resources["pipelines"].values()
    assert pipeline["name"] == defaults.pipeline_name
    volume_names = {v["name"] for v in resources["volumes"].values()}
    assert {defaults.raw_volume, defaults.archive_volume, defaults.state_volume} == volume_names
    # The pipeline reads only the raw Volume (D8).
    raw_root = pipeline["configuration"]["rearc.raw_root"]
    assert raw_root.endswith("${resources.volumes.raw.name}")
    assert "archive" not in raw_root and "state" not in raw_root
