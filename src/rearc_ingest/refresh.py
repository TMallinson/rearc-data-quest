"""Start a pipeline update when raw has changed since the last started update (D19)."""
from __future__ import annotations

from typing import Protocol

from databricks.sdk import WorkspaceClient


class PipelineTrigger(Protocol):
    def trigger(self) -> str:
        """Start an update and return its update id."""
        ...


class SdkPipelineTrigger:
    def __init__(self, client: WorkspaceClient, pipeline_name: str, *, development: bool) -> None:
        self._client = client
        self._name = pipeline_name
        self._development = development  # D28: set per update, not on the pipeline

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
        response = pipelines.start_update(
            pipeline_id=matches[0].pipeline_id, development=self._development
        )
        return response.update_id
