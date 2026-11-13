"""Parse the BLS directory listing (an IIS-style HTML page) into file entries.

Pure: no network. Strict by design. Any row that can't be parsed raises an error,
because a silently skipped row would later look like a removed file (D11).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urljoin


class ListingParseError(ValueError):
    """The listing isn't in the expected format; the run must not continue."""


@dataclass(frozen=True)
class ListingEntry:
    name: str
    timestamp_raw: str  # verbatim, e.g. "11/5/2026  8:30 AM"; never parsed (D7)
    size_raw: str  # verbatim byte count, e.g. "46068"
    url: str


@dataclass(frozen=True)
class Listing:
    entries: tuple[ListingEntry, ...]
    skipped_dirs: tuple[str, ...]


_PRE = re.compile(r"<pre>(.*?)</pre>", re.IGNORECASE | re.DOTALL)
_LINE_BREAK = re.compile(r"<br\s*/?>", re.IGNORECASE)
_PARENT_LINK = re.compile(r'<a href="[^"]*">\[To Parent Directory\]</a>', re.IGNORECASE)
_ROW = re.compile(
    r"(?P<timestamp>\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)\s+"
    r"(?P<size>\d+|&lt;dir&gt;)\s+"
    r'<a href="(?P<href>[^"]+)">(?P<name>[^<]+)</a>',
    re.IGNORECASE,
)
_SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def parse_listing(html: str, base_url: str) -> Listing:
    pre = _PRE.search(html)
    if pre is None:
        raise ListingParseError("no <pre> block found in the listing")

    entries: list[ListingEntry] = []
    skipped_dirs: list[str] = []
    for segment in _LINE_BREAK.split(pre.group(1)):
        line = segment.strip()
        if not line or _PARENT_LINK.fullmatch(line):
            continue
        row = _ROW.fullmatch(line)
        if row is None:
            raise ListingParseError(f"unparseable listing row: {line!r}")
        if row["size"].lower() == "&lt;dir&gt;":
            skipped_dirs.append(row["name"])
            continue
        entries.append(
            ListingEntry(
                name=row["name"],
                timestamp_raw=row["timestamp"],
                size_raw=row["size"],
                url=_file_url(base_url, row["href"], row["name"]),
            )
        )

    if not entries:
        raise ListingParseError("the listing contains no files")
    names = [entry.name for entry in entries]
    if len(set(names)) != len(names):
        raise ListingParseError(f"duplicate file names in the listing: {sorted(names)}")
    return Listing(tuple(entries), tuple(skipped_dirs))


def _file_url(base_url: str, href: str, name: str) -> str:
    """Resolve a link, refusing names or links that leave the listed folder."""
    if not _SAFE_NAME.fullmatch(name):
        raise ListingParseError(f"unsafe file name in the listing: {name!r}")
    url = urljoin(base_url, href)
    if url != urljoin(base_url, name):
        raise ListingParseError(f"link for {name!r} points outside the folder: {href!r}")
    return url
