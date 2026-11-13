"""The Bronze header check's failure paths (D13), which a successful update never exercises."""
import pytest

from utilities.bronze_contract import HeaderMismatchError, check_header

EXPECTED = ["series_id", "year", "period", "value", "footnote_codes"]


def test_matching_header_passes():
    check_header("pr.data.*", list(EXPECTED), EXPECTED)


@pytest.mark.parametrize("actual, difference", [
    (["series_id", "year", "period", "value"], "missing ['footnote_codes']"),
    (EXPECTED + ["extra_col"], "unexpected ['extra_col']"),
    (["year", "series_id", "period", "value", "footnote_codes"], "columns reordered"),
    (["series_id", "year", "period", "val", "footnote_codes"], "renamed column 'value' -> 'val'"),
    (["series_id", "year"], "missing ['period', 'value', 'footnote_codes']"),
], ids=["missing", "extra", "reordered", "renamed", "several-missing"])
def test_mismatch_raises_naming_the_file_and_the_difference(actual, difference):
    with pytest.raises(HeaderMismatchError) as caught:
        check_header("pr.data.*", actual, EXPECTED)
    message = str(caught.value)
    assert "pr.data.*" in message
    assert difference in message
