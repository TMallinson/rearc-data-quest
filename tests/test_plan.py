import pytest

from rearc_ingest.config import DATAUSA_KEY
from rearc_ingest.listing import ListingEntry
from rearc_ingest.manifest import Entry, Manifest
from rearc_ingest.plan import Reason, RemovalGuardError, plan_bls

TS = "11/5/2026  8:30 AM"


def listed(name, timestamp=TS, size="10"):
    return ListingEntry(name, timestamp, size, f"https://example.invalid/{name}")


def known(timestamp=TS, size="10", status="present"):
    return Entry("raw", "archive", "sha", "t", timestamp, size, status)


def plan(entries, files, *, max_removals=2, allow_removals=False):
    return plan_bls(
        tuple(entries),
        Manifest(files),
        max_removals=max_removals,
        protected_glob="pr.data.*",
        allow_removals=allow_removals,
    )


def test_new_changed_unchanged_and_reappeared():
    result = plan(
        [listed("pr.new"), listed("pr.ts", timestamp="2/5/2027  8:30 AM"),
         listed("pr.size", size="11"), listed("pr.same"), listed("pr.back")],
        {
            "bls/pr/pr.ts": known(),
            "bls/pr/pr.size": known(),
            "bls/pr/pr.same": known(),
            "bls/pr/pr.back": known(status="removed"),
        },
    )
    reasons = {d.key: d.reason for d in result.downloads}
    assert reasons == {
        "bls/pr/pr.new": Reason.NEW,
        "bls/pr/pr.ts": Reason.CHANGED,
        "bls/pr/pr.size": Reason.CHANGED,
        "bls/pr/pr.back": Reason.REAPPEARED,
    }
    assert result.unchanged == ("bls/pr/pr.same",)
    assert result.removals == ()


def test_comparison_is_exact_string_equality():
    # Same instant, different formatting: still treated as changed (D7: no timestamp parsing).
    result = plan([listed("pr.a", timestamp="11/5/2026 8:30 AM")], {"bls/pr/pr.a": known()})
    assert [d.reason for d in result.downloads] == [Reason.CHANGED]


def test_removals_within_the_limit_are_planned():
    result = plan([listed("pr.keep")], {
        "bls/pr/pr.keep": known(), "bls/pr/pr.a": known(), "bls/pr/pr.b": known(),
    })
    assert [r.name for r in result.removals] == ["pr.a", "pr.b"]


def test_more_than_the_limit_is_refused():
    files = {f"bls/pr/pr.{c}": known() for c in "abc"} | {"bls/pr/pr.keep": known()}
    with pytest.raises(RemovalGuardError, match="exceeds the limit of 2"):
        plan([listed("pr.keep")], files)


def test_the_limit_is_a_setting():
    files = {f"bls/pr/pr.{c}": known() for c in "abc"} | {"bls/pr/pr.keep": known()}
    result = plan([listed("pr.keep")], files, max_removals=3)
    assert len(result.removals) == 3


def test_any_protected_removal_is_refused():
    with pytest.raises(RemovalGuardError, match="protected"):
        plan([listed("pr.keep")], {"bls/pr/pr.keep": known(), "bls/pr/pr.data.0.Current": known()})


def test_allow_removals_bypasses_the_guard():
    files = {f"bls/pr/pr.data.{i}": known() for i in range(3)} | {"bls/pr/pr.keep": known()}
    result = plan([listed("pr.keep")], files, allow_removals=True)
    assert len(result.removals) == 3


def test_already_removed_and_non_bls_entries_are_not_removed_again():
    result = plan([listed("pr.keep")], {
        "bls/pr/pr.keep": known(),
        "bls/pr/pr.gone": known(status="removed"),
        DATAUSA_KEY: Entry("raw", "archive", "sha", "t"),
    })
    assert result.removals == ()
