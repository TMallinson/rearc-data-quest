"""Settings for an ingestion run.

Everything a run needs is here, so nothing is a local-only assumption (D1):
Volume locations are Unity Catalog paths, and the only machine-specific input is
the BLS_USER_AGENT environment variable.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import metadata

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
DATAUSA_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)

# Manifest keys double as paths relative to the raw and archive Volume roots.
BLS_PREFIX = "bls/pr/"
DATAUSA_KEY = "datausa/population.json"


class ConfigError(Exception):
    """The run can't start: configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    bls_user_agent: str  # carries contact info, as BLS asks
    datausa_user_agent: str  # tool name plus project URL, no personal email (D20)
    catalog: str = "workspace"
    schema: str = "rearc_quest"
    raw_volume: str = "raw"
    archive_volume: str = "archive"
    state_volume: str = "state"
    bls_base_url: str = BLS_BASE_URL
    datausa_url: str = DATAUSA_URL
    # D11: removal guard.
    max_removals_without_flag: int = 2
    protected_removal_glob: str = "pr.data.*"
    # D19: the pipeline to refresh. Must match resources/rearc_quest.pipeline.yml (tested).
    pipeline_name: str | None = "rearc_quest_pipeline"
    # D28: start each update in development mode (no automatic retries, faster restarts).
    pipeline_development: bool = True
    # Politeness and retry policy (CLAUDE.md). max_attempts also bounds size-mismatch retries (D21).
    request_pause_seconds: float = 1.0
    max_attempts: int = 4
    backoff_base_seconds: float = 2.0
    timeout_seconds: float = 60.0

    def _volume_root(self, volume: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{volume}"

    @property
    def raw_root(self) -> str:
        return self._volume_root(self.raw_volume)

    @property
    def archive_root(self) -> str:
        return self._volume_root(self.archive_volume)

    @property
    def manifest_path(self) -> str:
        return f"{self._volume_root(self.state_volume)}/manifest.json"


def load_user_agent(environ: Mapping[str, str] = os.environ) -> str:
    """Read BLS_USER_AGENT, refusing to start without contact info in it."""
    user_agent = environ.get("BLS_USER_AGENT", "").strip()
    if not user_agent:
        raise ConfigError("BLS_USER_AGENT is not set. Copy .env.example to .env and fill it in.")
    if "@" not in user_agent:
        raise ConfigError(
            "BLS_USER_AGENT must include contact info (an email address); "
            "BLS blocks automated requests without it."
        )
    return user_agent


def tool_user_agent() -> str:
    """User-Agent for DataUSA: tool name, version and project URL from package metadata (D20)."""
    try:
        package = metadata.metadata("rearc-ingest")
    except metadata.PackageNotFoundError as error:
        raise ConfigError("rearc-ingest is not installed; run: pip install -e '.[dev]'") from error
    urls = dict(entry.split(", ", 1) for entry in package.get_all("Project-URL") or [])
    repository = urls.get("Repository")
    if not repository:
        raise ConfigError(
            "Set [project.urls] Repository in pyproject.toml and reinstall; "
            "DataUSA requests identify the tool with it."
        )
    return f"rearc-ingest/{package['Version']} (+{repository})"
