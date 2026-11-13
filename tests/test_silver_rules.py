"""Pure Silver rules: the DECIMAL fit pattern (D16/D33), exact AllData match (D30), labels (D5)."""
import re

import pytest

from utilities.silver_rules import (
    ALLDATA_FILE, NUMERIC_PATTERN, SERIES_ID_PATTERN, VALUE_FITS_PATTERN, WHOLE_NUMBER_PATTERN,
    base_name, decimal_fits_pattern, fallback_label,
)


@pytest.mark.parametrize("text, fits", [
    ("25576.642", True), ("-60.8", True), ("0", True), ("999999999.999", True),
    ("1.2345", False),  # 4 decimal places: would be silently rounded by a cast
    ("1234567890.1", False),  # 10 integer digits: overflows DECIMAL(12,3)
])
def test_value_fits_decimal_12_3(text, fits):
    assert bool(re.fullmatch(VALUE_FITS_PATTERN, text)) is fits
    assert re.fullmatch(NUMERIC_PATTERN, text)  # all are numeric: fit is a separate question


def test_fit_pattern_follows_precision_and_scale():
    assert decimal_fits_pattern(5, 2) == "^-?[0-9]{1,3}([.][0-9]{1,2})?$"


@pytest.mark.parametrize("text", ["", "-", "1.", ".5", "1,000", "N/A"])
def test_non_numeric_text_is_rejected(text):
    assert not re.fullmatch(NUMERIC_PATTERN, text)


def test_series_id_pattern():
    assert re.fullmatch(SERIES_ID_PATTERN, "PRS30006032")
    assert not re.fullmatch(SERIES_ID_PATTERN, "PRS30006032      ")  # untrimmed
    assert not re.fullmatch(SERIES_ID_PATTERN, "PRX30006032")


@pytest.mark.parametrize("text, whole", [("316128839.0", True), ("316128839", True), ("316128839.5", False)])
def test_whole_number_pattern(text, whole):
    assert bool(re.fullmatch(WHOLE_NUMBER_PATTERN, text)) is whole


@pytest.mark.parametrize("path, selected", [
    ("dbfs:/Volumes/workspace/rearc_quest/raw/bls/pr/pr.data.1.AllData", True),
    ("dbfs:/Volumes/workspace/rearc_quest/raw/bls/pr/pr.data.1.AllData.bak", False),
    ("dbfs:/Volumes/workspace/rearc_quest/raw/bls/pr/old.pr.data.1.AllData", False),
    ("dbfs:/Volumes/workspace/rearc_quest/raw/bls/pr/pr.data.0.Current", False),
])
def test_alldata_is_matched_by_exact_base_name(path, selected):
    assert (base_name(path) == ALLDATA_FILE) is selected


def test_fallback_label_mirrors_bls_titles_and_needs_every_part():
    assert fallback_label("Hours worked", "Manufacturing", "% Change from previous quarter") == (
        "Hours worked for the Manufacturing sector, % Change from previous quarter"
    )
    assert fallback_label("Hours worked", None, "Index (2017=100)") is None
