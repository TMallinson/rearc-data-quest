import pytest

from fakes import listing_line, make_listing_html
from rearc_ingest.config import BLS_BASE_URL
from rearc_ingest.listing import ListingParseError, parse_listing


def parse(rows):
    return parse_listing(make_listing_html(rows), BLS_BASE_URL)


def test_parses_rows_and_keeps_timestamp_and_size_verbatim():
    listing = parse([
        listing_line("pr.class", "11/5/2026 8:30 AM", "102"),
        listing_line("pr.period", "1/7/1994 2:53 PM", "146"),
    ])
    first, second = listing.entries
    assert first.name == "pr.class"
    assert first.timestamp_raw == "11/5/2026  8:30 AM"  # double space kept, no parsing
    assert first.size_raw == "102"
    assert first.url == BLS_BASE_URL + "pr.class"
    assert second.timestamp_raw == "1/7/1994  2:53 PM"


def test_parent_link_and_blank_segments_are_ignored():
    listing = parse([listing_line("pr.txt", "11/17/2011 4:11 PM", "18343")])
    assert [e.name for e in listing.entries] == ["pr.txt"]


def test_directory_rows_are_skipped_and_reported():
    dir_row = ' 1/1/2026  1:00 PM        &lt;dir&gt; <A HREF="/pub/time.series/pr/old/">old</A><br>'
    listing = parse([listing_line("pr.txt", "11/17/2011 4:11 PM", "18343"), dir_row])
    assert [e.name for e in listing.entries] == ["pr.txt"]
    assert listing.skipped_dirs == ("old",)


def test_unparseable_row_raises_instead_of_being_skipped():
    rows = [listing_line("pr.txt", "11/17/2011 4:11 PM", "18343"), "something unexpected<br>"]
    with pytest.raises(ListingParseError, match="unparseable"):
        parse(rows)


def test_empty_listing_raises():
    with pytest.raises(ListingParseError, match="no files"):
        parse([])


def test_missing_pre_block_raises():
    with pytest.raises(ListingParseError, match="<pre>"):
        parse_listing("<html><body>Service unavailable</body></html>", BLS_BASE_URL)


def test_link_outside_the_folder_raises():
    row = ' 11/5/2026  8:30 AM          102 <A HREF="/pub/other/pr.class">pr.class</A><br>'
    with pytest.raises(ListingParseError, match="outside the folder"):
        parse([row])


def test_unsafe_name_raises():
    row = ' 11/5/2026  8:30 AM          102 <A HREF="/pub/time.series/pr/..">..</A><br>'
    with pytest.raises(ListingParseError, match="unsafe"):
        parse([row])


def test_duplicate_names_raise():
    line = listing_line("pr.txt", "11/17/2011 4:11 PM", "18343")
    with pytest.raises(ListingParseError, match="duplicate"):
        parse([line, line])
