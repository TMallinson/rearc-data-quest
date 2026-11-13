"""Command-line entry point: `rearc-ingest`.

Exit codes: 0 success; 1 failed (fetch, parse, size or manifest error); 2 configuration
error; 3 stopped by HTTP 403; 4 refused by the removal guard; 5 ingested but the
pipeline update couldn't be started (still pending).
"""
from __future__ import annotations

import argparse
import dataclasses
import sys

from dotenv import find_dotenv, load_dotenv

from .config import ConfigError, Settings, load_user_agent, tool_user_agent
from .http import FetchError, Fetcher, ForbiddenError
from .listing import ListingParseError
from .manifest import ManifestError
from .plan import RemovalGuardError
from .run import RefreshError, run
from .summary import RunSummary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rearc-ingest",
        description="Land BLS pr files and DataUSA population into the raw Volume.",
    )
    parser.add_argument("--profile", default="rearc", help="Databricks CLI profile (default: rearc)")
    parser.add_argument("--catalog", help="override the catalog (default: workspace)")
    parser.add_argument("--schema", help="override the schema (default: rearc_quest)")
    parser.add_argument("--dry-run", action="store_true", help="fetch the listing and show the plan only")
    parser.add_argument("--allow-removals", action="store_true", help="permit removals beyond the guard (D11)")
    parser.add_argument("--max-removals", type=int, help="removals allowed without --allow-removals (default: 2)")
    parser.add_argument("--no-refresh", action="store_true", help="don't start a pipeline update (D19)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    load_dotenv(find_dotenv(usecwd=True))  # local convenience; never overrides real env vars

    try:
        settings = _settings(args)
    except ConfigError as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        return 2
    refresh = not args.no_refresh and not args.dry_run
    if refresh and settings.pipeline_name is None:
        print(
            "Configuration error: no pipeline is configured to refresh yet. Use --no-refresh.",
            file=sys.stderr,
        )
        return 2

    # Imported here so --help and config errors need no Databricks setup.
    from databricks.sdk import WorkspaceClient

    from .refresh import SdkPipelineTrigger
    from .volume import SdkVolume

    client = WorkspaceClient(profile=args.profile)
    summary = RunSummary()
    try:
        run(
            settings,
            summary,
            bls_fetcher=_fetcher(settings, settings.bls_user_agent),
            datausa_fetcher=_fetcher(settings, settings.datausa_user_agent),
            volume=SdkVolume(client),
            trigger=(
                SdkPipelineTrigger(
                    client, settings.pipeline_name, development=settings.pipeline_development
                )
                if refresh
                else None
            ),
            allow_removals=args.allow_removals,
            dry_run=args.dry_run,
            refresh=refresh,
        )
    except ForbiddenError as error:
        return _fail(summary, f"STOPPED (403): {error}", 3)
    except RemovalGuardError as error:
        return _fail(summary, f"REFUSED: {error}", 4)
    except RefreshError as error:
        return _fail(summary, f"REFRESH NOT STARTED: {error}", 5)
    except ManifestError as error:
        return _fail(
            summary,
            f"FAILED: {error}. To recover, delete {settings.manifest_path} and re-run; "
            "every file is re-landed safely (D25).",
            1,
        )
    except (FetchError, ListingParseError) as error:
        return _fail(summary, f"FAILED: {error}", 1)
    print(summary.render())
    return 0


def _settings(args: argparse.Namespace) -> Settings:
    overrides = {
        name: value
        for name, value in [
            ("catalog", args.catalog),
            ("schema", args.schema),
            ("max_removals_without_flag", args.max_removals),
        ]
        if value is not None
    }
    settings = Settings(bls_user_agent=load_user_agent(), datausa_user_agent=tool_user_agent())
    return dataclasses.replace(settings, **overrides)


def _fetcher(settings: Settings, user_agent: str) -> Fetcher:
    return Fetcher(
        user_agent,
        timeout=settings.timeout_seconds,
        max_attempts=settings.max_attempts,
        backoff_base=settings.backoff_base_seconds,
        pause=settings.request_pause_seconds,
    )


def _fail(summary: RunSummary, message: str, code: int) -> int:
    print(message, file=sys.stderr)
    print(summary.render(), file=sys.stderr)
    if summary.raw_changed:
        print(
            "NOTE: raw changed before the failure and is marked refresh-pending; "
            "the next successful run will start a pipeline update.",
            file=sys.stderr,
        )
    return code
