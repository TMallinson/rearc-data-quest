"""D20: the DataUSA User-Agent comes from package metadata, never from personal details."""
from email.message import Message

import pytest

from rearc_ingest import config


def fake_metadata(project_urls):
    package = Message()
    package["Version"] = "0.1.0"
    for url in project_urls:
        package["Project-URL"] = url
    return lambda name: package


def test_tool_user_agent_uses_version_and_repository_url(monkeypatch):
    monkeypatch.setattr(config.metadata, "metadata",
                        fake_metadata(["Repository, https://example.invalid/repo"]))
    assert config.tool_user_agent() == "rearc-ingest/0.1.0 (+https://example.invalid/repo)"


def test_tool_user_agent_without_repository_url_is_refused(monkeypatch):
    monkeypatch.setattr(config.metadata, "metadata", fake_metadata([]))
    with pytest.raises(config.ConfigError, match="Repository"):
        config.tool_user_agent()
