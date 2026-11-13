"""Start a pipeline update after a run that changed raw (D19). Wired up in Phase 4."""
from __future__ import annotations

from typing import Protocol

from databricks.sdk import WorkspaceClient


class PipelineTrigger(Protocol):
    def trigger(self) -> str:
        """Start an update and return its update id."""
        ...


class SdkPipelineTrigger:
    def __init__(self, client: WorkspaceClient, pipeline_name: str) -> None:
        self._client = client
        self._name = pipeline_name

    def trigger(self) -> str:
        pipelines = self._client.pipelines
        matches = [
            p
            for p in pipelines.list_pipelines(filter=f"name LIKE '{self._name}'")
            if p.name == self._name
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"expected exactly one pipeline named {self._name!r}, found {len(matches)}"
            )
        return pipelines.start_update(pipeline_id=matches[0].pipeline_id).update_id
