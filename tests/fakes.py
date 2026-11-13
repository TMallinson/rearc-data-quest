"""Test doubles: an in-memory Volume, a pipeline trigger, and a fake HTTP server.

All content here is synthetic, shaped like the real sources but not copied from them.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field

from rearc_ingest.config import BLS_BASE_URL, DATAUSA_URL

BLS_PATH = "/pub/time.series/pr/"


class SimulatedCrash(RuntimeError):
    pass


@dataclass
class FakeVolume:
    files: dict[str, bytes] = field(default_factory=dict)
    ops: list[tuple[str, str]] = field(default_factory=list)  # ("write" | "delete", path)
    crash_on_write: Callable[[str], bool] | None = None

    def read_bytes(self, path: str) -> bytes | None:
        return self.files.get(path)

    def write_bytes(self, path: str, data: bytes) -> None:
        if self.crash_on_write and self.crash_on_write(path):
            raise SimulatedCrash(f"simulated crash writing {path}")
        self.files[path] = bytes(data)
        self.ops.append(("write", path))

    def delete(self, path: str) -> None:
        self.files.pop(path, None)
        self.ops.append(("delete", path))


@dataclass
class FakeTrigger:
    calls: int = 0

    def trigger(self) -> str:
        self.calls += 1
        return f"fake-update-{self.calls}"


@dataclass
class FakeResponse:
    status_code: int
    content: bytes = b""


class FakeSession:
    """Stands in for requests.Session: routes each GET to a handler and records it."""

    def __init__(self, handler: Callable[[str], FakeResponse]) -> None:
        self.handler = handler
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, headers: dict[str, str] | None = None, timeout: float | None = None):
        self.calls.append((url, dict(headers or {})))
        return self.handler(url)


def listing_line(name: str, timestamp: str, size: str) -> str:
    """One row in the IIS layout: right-aligned date, time and size columns."""
    date, time_, ampm = timestamp.split()
    return f'{date:>10} {time_ + " " + ampm:>8} {size:>12} <A HREF="{BLS_PATH}{name}">{name}</A><br>'


def make_listing_html(rows: list[str]) -> str:
    return (
        "<html><head><title>download.bls.gov - /pub/time.series/pr/</title></head><body>"
        "<H1>download.bls.gov - /pub/time.series/pr/</H1><hr>\n\n<pre>"
        '<A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>'
        + "".join(rows)
        + "</pre><hr></body></html>"
    )


SOURCE_NAMES = [
    "pr.class", "pr.contacts", "pr.data.0.Current", "pr.data.1.AllData", "pr.duration",
    "pr.footnote", "pr.measure", "pr.period", "pr.seasonal", "pr.sector", "pr.series", "pr.txt",
]


class FakeSources:
    """A fake BLS folder plus a fake DataUSA endpoint, editable between runs."""

    def __init__(self) -> None:
        self.bls: dict[str, tuple[str, bytes]] = {
            name: ("11/5/2026  8:30 AM", f"synthetic {name}\tv1\r\n".encode()) for name in SOURCE_NAMES
        }
        self.datausa = json.dumps({"data": [{"Year": 2013, "Population": 1.0}]}).encode()
        self.overrides: dict[str, list[FakeResponse | Exception]] = {}

    def set_file(self, name: str, content: bytes, timestamp: str = "2/5/2027  8:30 AM") -> None:
        self.bls[name] = (timestamp, content)

    def touch(self, name: str, timestamp: str = "2/5/2027  8:30 AM") -> None:
        self.bls[name] = (timestamp, self.bls[name][1])

    def handler(self, url: str) -> FakeResponse:
        queued = self.overrides.get(url)
        if queued:
            item = queued.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        if url == BLS_BASE_URL:
            rows = [listing_line(n, ts, str(len(c))) for n, (ts, c) in sorted(self.bls.items())]
            return FakeResponse(200, make_listing_html(rows).encode())
        if url.startswith(BLS_BASE_URL) and url[len(BLS_BASE_URL):] in self.bls:
            return FakeResponse(200, self.bls[url[len(BLS_BASE_URL):]][1])
        if url == DATAUSA_URL:
            return FakeResponse(200, self.datausa)
        return FakeResponse(404)
