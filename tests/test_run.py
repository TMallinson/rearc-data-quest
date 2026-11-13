"""End-to-end ingestion against fakes: D7, D10, D11, D19 and the 'writes nothing' proof."""
import json

import pytest

from conftest import TEST_TOOL_AGENT, TEST_USER_AGENT
from fakes import FakeResponse, SimulatedCrash
from rearc_ingest.config import BLS_BASE_URL, DATAUSA_URL
from rearc_ingest.http import FetchError, ForbiddenError
from rearc_ingest.listing import ListingParseError
from rearc_ingest.manifest import Manifest
from rearc_ingest.plan import RemovalGuardError
from rearc_ingest.run import RefreshError
from rearc_ingest.sources.bls import SizeMismatchError

RAW = "/Volumes/workspace/rearc_quest/raw/"
ARCHIVE = "/Volumes/workspace/rearc_quest/archive/"
MANIFEST = "/Volumes/workspace/rearc_quest/state/manifest.json"


def manifest_of(world) -> Manifest:
    return Manifest.from_json(world.volume.files[MANIFEST])


def archive_paths(world, key):
    return sorted(p for p in world.volume.files if p.startswith(f"{ARCHIVE}{key}/"))


def test_first_run_lands_everything_and_refreshes_once(world):
    summary = world.run()
    assert summary.bls_downloads == 12
    assert summary.datausa_fetches == 1
    # 13 manifest writes (one per file) + 1 to clear refresh_pending after the update starts
    assert (summary.raw_writes, summary.archive_writes, summary.manifest_writes) == (13, 13, 14)
    assert world.trigger.calls == 1
    assert manifest_of(world).refresh_pending is False
    for name, (_, content) in world.sources.bls.items():
        assert world.volume.files[f"{RAW}bls/pr/{name}"] == content
    assert world.volume.files[f"{RAW}datausa/population.json"] == world.sources.datausa
    entry = manifest_of(world).files["bls/pr/pr.series"]
    assert (entry.listing_timestamp, entry.listing_size, entry.status) == (
        "11/5/2026  8:30 AM", str(len(world.sources.bls["pr.series"][1])), "present")


def test_write_order_is_archive_then_raw_then_manifest(world):
    world.run()
    first_three = [path for _, path in world.volume.ops[:3]]
    assert first_three[0].startswith(ARCHIVE)
    assert first_three[1].startswith(RAW)
    assert first_three[2] == MANIFEST


def test_second_run_with_no_source_changes_writes_nothing(world):
    world.run()
    ops_before, requests_before = len(world.volume.ops), len(world.session.calls)
    summary = world.run()
    assert summary.total_writes == 0
    assert len(world.volume.ops) == ops_before
    assert summary.bls_downloads == 0
    assert world.urls_requested()[requests_before:] == [BLS_BASE_URL, DATAUSA_URL]
    assert world.trigger.calls == 1  # only the first run's
    assert summary.refresh_triggered is False


def test_changed_file_is_archived_and_rewritten(world):
    world.run()
    world.sources.set_file("pr.series", b"synthetic pr.series\tv2\r\n")
    summary = world.run()
    assert summary.bls_downloads == 1
    assert (summary.archive_writes, summary.raw_writes, summary.manifest_writes) == (1, 1, 2)
    assert world.volume.files[f"{RAW}bls/pr/pr.series"] == b"synthetic pr.series\tv2\r\n"
    assert len(archive_paths(world, "bls/pr/pr.series")) == 2  # both versions kept
    assert world.trigger.calls == 2


def test_touched_file_updates_only_listing_metadata_then_goes_quiet(world):
    world.run()
    world.sources.touch("pr.class")
    summary = world.run()
    assert summary.bls_downloads == 1
    assert (summary.archive_writes, summary.raw_writes, summary.manifest_writes) == (0, 0, 1)
    assert summary.touched == ["bls/pr/pr.class"]
    assert world.trigger.calls == 1  # raw unchanged: no refresh
    assert manifest_of(world).files["bls/pr/pr.class"].listing_timestamp == "2/5/2027  8:30 AM"
    # The fix from D7: the next run doesn't download it again.
    assert world.run().bls_downloads == 0


def test_removal_within_the_limit_mirrors_the_source(world):
    world.run()
    for name in ("pr.contacts", "pr.footnote"):
        del world.sources.bls[name]
    summary = world.run()
    assert summary.raw_deletes == 2
    for name in ("pr.contacts", "pr.footnote"):
        key = f"bls/pr/{name}"
        assert f"{RAW}{key}" not in world.volume.files
        assert len(archive_paths(world, key)) == 1  # archive keeps it
        assert manifest_of(world).files[key].status == "removed"
    assert world.trigger.calls == 2


@pytest.mark.parametrize("removed", [
    ["pr.contacts", "pr.footnote", "pr.class"],  # more than 2
    ["pr.data.0.Current"],  # protected
])
def test_guarded_removals_change_nothing_without_the_flag(world, removed):
    world.run()
    for name in removed:
        del world.sources.bls[name]
    world.sources.set_file("pr.series", b"changed in the same run")
    files_before, ops_before = dict(world.volume.files), len(world.volume.ops)
    with pytest.raises(RemovalGuardError):
        world.run()
    assert world.volume.files == files_before
    assert len(world.volume.ops) == ops_before
    summary = world.run(allow_removals=True)
    assert summary.raw_deletes == len(removed)


def test_removal_limit_is_a_setting(world):
    world.run()
    for name in ("pr.contacts", "pr.footnote", "pr.class"):
        del world.sources.bls[name]
    summary = world.run(settings=world.settings(max_removals_without_flag=3))
    assert summary.raw_deletes == 3


def test_file_that_reappears_is_written_back_even_with_the_same_content(world):
    world.run()
    original = world.sources.bls.pop("pr.contacts")
    world.run()
    world.sources.bls["pr.contacts"] = original
    summary = world.run()
    assert summary.raw_writes == 1
    assert world.volume.files[f"{RAW}bls/pr/pr.contacts"] == original[1]
    assert manifest_of(world).files["bls/pr/pr.contacts"].status == "present"


def test_crash_between_raw_and_manifest_recovers_on_rerun(world):
    world.volume.crash_on_write = lambda path: path == MANIFEST
    with pytest.raises(SimulatedCrash):
        world.run()
    assert MANIFEST not in world.volume.files  # the first file's raw copy landed, unrecorded
    world.volume.crash_on_write = None
    summary = world.run()
    assert summary.raw_writes == 13
    assert len(manifest_of(world).files) == 13
    assert world.run().total_writes == 0


def test_deleted_manifest_relands_identical_bytes_and_sets_pending(world):
    world.run()
    before = {path: data for path, data in world.volume.files.items() if path != MANIFEST}
    del world.volume.files[MANIFEST]  # D25 recovery: delete the manifest and re-run
    summary = world.run(refresh=False, trigger=None)
    after = {path: data for path, data in world.volume.files.items() if path != MANIFEST}
    assert after == before  # same paths, byte-identical raw and archive content
    assert summary.bls_downloads == 12  # everything re-landed
    assert manifest_of(world).refresh_pending is True
    assert len(manifest_of(world).files) == 13


def test_no_refresh_never_triggers_and_leaves_refresh_pending(world):
    summary = world.run(refresh=False, trigger=None)
    assert summary.raw_writes == 13
    assert world.trigger.calls == 0
    assert manifest_of(world).refresh_pending is True
    # Another --no-refresh run with no changes: writes nothing, flag still set.
    again = world.run(refresh=False, trigger=None)
    assert again.total_writes == 0
    assert manifest_of(world).refresh_pending is True
    # The first run with refresh enabled starts the update even though nothing changed.
    summary = world.run()
    assert (summary.raw_writes, summary.manifest_writes) == (0, 1)
    assert world.trigger.calls == 1
    assert manifest_of(world).refresh_pending is False


def test_failure_after_a_raw_write_leaves_refresh_pending_for_the_next_run(world):
    world.run()
    assert world.trigger.calls == 1
    world.sources.set_file("pr.series", b"synthetic pr.series\tv2\r\n")
    world.sources.overrides[DATAUSA_URL] = [FakeResponse(500)] * 4  # fails after the BLS write
    with pytest.raises(FetchError):
        world.run()
    assert world.volume.files[f"{RAW}bls/pr/pr.series"] == b"synthetic pr.series\tv2\r\n"
    assert manifest_of(world).refresh_pending is True
    assert world.trigger.calls == 1  # no update on a failed run
    # Next run: no source changes, but raw changed since the last started update.
    summary = world.run()
    assert (summary.raw_writes, summary.raw_deletes) == (0, 0)
    assert summary.refresh_triggered is True
    assert world.trigger.calls == 2
    assert manifest_of(world).refresh_pending is False
    # And then the steady state writes nothing again.
    assert world.run().total_writes == 0


def test_failed_trigger_keeps_refresh_pending(world):
    class BrokenTrigger:
        def trigger(self):
            raise RuntimeError("API unavailable")

    with pytest.raises(RefreshError, match="next run will try again"):
        world.run(trigger=BrokenTrigger())
    assert manifest_of(world).refresh_pending is True
    world.run()
    assert world.trigger.calls == 1
    assert manifest_of(world).refresh_pending is False


def test_refresh_without_a_trigger_fails_before_any_request(world):
    with pytest.raises(ValueError, match="no pipeline trigger"):
        world.run(trigger=None)
    assert world.session.calls == []


def test_dry_run_writes_nothing_and_downloads_no_files(world):
    summary = world.run(dry_run=True)
    assert summary.total_writes == 0
    assert world.volume.ops == []
    assert world.urls_requested() == [BLS_BASE_URL]
    assert len(summary.planned_downloads) == 13
    assert world.trigger.calls == 0


def test_403_stops_the_run_with_no_retry_and_no_further_requests(world):
    url = BLS_BASE_URL + "pr.class"
    world.sources.overrides[url] = [FakeResponse(403)]
    with pytest.raises(ForbiddenError):
        world.run()
    assert world.urls_requested().count(url) == 1
    assert world.urls_requested()[-1] == url
    assert DATAUSA_URL not in world.urls_requested()
    assert world.trigger.calls == 0


def test_bad_listing_writes_nothing(world):
    world.sources.overrides[BLS_BASE_URL] = [FakeResponse(200, b"<html>maintenance</html>")]
    with pytest.raises(ListingParseError):
        world.run()
    assert world.volume.ops == []


@pytest.mark.parametrize("body", [b"trunc", b"x" * 500], ids=["shorter", "longer"])
def test_size_mismatch_is_retried(world, body):
    url = BLS_BASE_URL + "pr.series"
    world.sources.overrides[url] = [FakeResponse(200, body)]
    summary = world.run()
    assert world.urls_requested().count(url) == 2
    assert world.volume.files[f"{RAW}bls/pr/pr.series"] == world.sources.bls["pr.series"][1]
    assert summary.raw_writes == 13


def test_persistent_size_mismatch_fails_and_never_writes_the_file(world):
    url = BLS_BASE_URL + "pr.series"
    world.sources.overrides[url] = [FakeResponse(200, b"x" * 500)] * 4
    with pytest.raises(SizeMismatchError, match="may have changed on the server during this run"):
        world.run()
    assert world.urls_requested().count(url) == 4  # max_attempts
    assert f"{RAW}bls/pr/pr.series" not in world.volume.files
    assert archive_paths(world, "bls/pr/pr.series") == []
    assert "bls/pr/pr.series" not in manifest_of(world).files


def test_each_source_gets_its_own_user_agent(world):
    world.run()
    agents = {url: headers["User-Agent"] for url, headers in world.session.calls}
    assert agents[BLS_BASE_URL] == TEST_USER_AGENT
    assert agents[DATAUSA_URL] == TEST_TOOL_AGENT


def test_invalid_datausa_payload_is_never_landed(world):
    world.sources.overrides[DATAUSA_URL] = [FakeResponse(200, json.dumps({"error": "x"}).encode())] * 4
    with pytest.raises(FetchError, match="no 'data' list"):
        world.run()
    assert f"{RAW}datausa/population.json" not in world.volume.files
    assert world.trigger.calls == 0


def test_datausa_unchanged_bytes_are_not_rewritten(world):
    world.run()
    assert world.run().total_writes == 0  # same bytes: fetched, hashed, not rewritten
    world.sources.datausa = json.dumps({"data": [{"Year": 2014, "Population": 2.0}]}).encode()
    summary = world.run()
    assert summary.written == ["datausa/population.json"]
    assert world.trigger.calls == 2
