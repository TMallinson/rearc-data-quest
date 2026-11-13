"""What a run did, counted at each write so the 'writes nothing' proof is exact."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunSummary:
    dry_run: bool = False
    listed_files: int = 0
    skipped_dirs: list[str] = field(default_factory=list)
    # Requests
    bls_downloads: int = 0
    datausa_fetches: int = 0
    # Writes to the Volumes
    archive_writes: int = 0
    raw_writes: int = 0
    raw_deletes: int = 0
    manifest_writes: int = 0
    # Per-file outcomes (manifest keys)
    written: list[str] = field(default_factory=list)
    touched: list[str] = field(default_factory=list)  # listing changed, content didn't
    unchanged: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    planned_downloads: list[str] = field(default_factory=list)  # dry run only
    planned_removals: list[str] = field(default_factory=list)  # dry run only
    # D19
    refresh_triggered: bool = False
    pipeline_update_id: str | None = None
    refresh_pending: bool = False  # state of the manifest flag at the end of the run

    @property
    def raw_changed(self) -> bool:
        return self.raw_writes + self.raw_deletes > 0

    @property
    def total_writes(self) -> int:
        return self.archive_writes + self.raw_writes + self.raw_deletes + self.manifest_writes

    def render(self) -> str:
        lines = [
            "DRY RUN: nothing downloaded or written" if self.dry_run else "Ingestion run",
            f"  listed files:      {self.listed_files}",
            f"  BLS downloads:     {self.bls_downloads}",
            f"  DataUSA fetches:   {self.datausa_fetches}",
            f"  archive writes:    {self.archive_writes}",
            f"  raw writes:        {self.raw_writes}",
            f"  raw deletes:       {self.raw_deletes}",
            f"  manifest writes:   {self.manifest_writes}",
            f"  total writes:      {self.total_writes}",
        ]
        for label, keys in [
            ("written", self.written),
            ("touched (metadata only)", self.touched),
            ("removed", self.removed),
            ("would download", self.planned_downloads),
            ("would remove", self.planned_removals),
            ("skipped directories", self.skipped_dirs),
        ]:
            if keys:
                lines.append(f"  {label}: {', '.join(keys)}")
        if self.refresh_triggered:
            lines.append(f"  pipeline update started: {self.pipeline_update_id}")
        elif not self.dry_run:
            lines.append("  pipeline update: not started")
        if self.refresh_pending:
            lines.append("  refresh pending: yes (the next run with refresh enabled starts an update)")
        return "\n".join(lines)
