from __future__ import annotations

import dataclasses
import itertools

import pytest

from fakes import FakeSession, FakeSources, FakeTrigger, FakeVolume
from rearc_ingest.config import Settings
from rearc_ingest.http import Fetcher
from rearc_ingest.run import run
from rearc_ingest.summary import RunSummary

TEST_USER_AGENT = "Test Person (test@example.com)"
TEST_TOOL_AGENT = "rearc-ingest/test (+https://example.invalid/repo)"


class World:
    """Fake sources, Volume and trigger that persist across several runs."""

    def __init__(self) -> None:
        self.sources = FakeSources()
        self.volume = FakeVolume()
        self.trigger = FakeTrigger()
        self.session = FakeSession(self.sources.handler)
        self._clock = itertools.count()  # deterministic timestamps for written_at

    def settings(self, **overrides) -> Settings:
        base = Settings(bls_user_agent=TEST_USER_AGENT, datausa_user_agent=TEST_TOOL_AGENT)
        return dataclasses.replace(base, **overrides)

    def fetcher(self, user_agent: str) -> Fetcher:
        return Fetcher(user_agent, session=self.session, pause=0, sleep=lambda _: None)

    def run(self, *, settings: Settings | None = None, **kwargs) -> RunSummary:
        settings = settings or self.settings()
        summary = RunSummary()
        kwargs.setdefault("trigger", self.trigger)
        run(
            settings,
            summary,
            bls_fetcher=self.fetcher(settings.bls_user_agent),
            datausa_fetcher=self.fetcher(settings.datausa_user_agent),
            volume=self.volume,
            now=lambda: f"2026-11-13T00:00:{next(self._clock):02d}+00:00",
            **kwargs,
        )
        return summary

    def urls_requested(self) -> list[str]:
        return [url for url, _ in self.session.calls]


@pytest.fixture
def world() -> World:
    return World()
