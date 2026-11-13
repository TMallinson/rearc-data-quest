"""SdkPipelineTrigger finds the pipeline by exact name and starts a development-mode update (D28)."""
from types import SimpleNamespace

import pytest

from rearc_ingest.refresh import SdkPipelineTrigger


class FakePipelinesAPI:
    def __init__(self, pipelines):
        self._pipelines = pipelines
        self.started = []

    def list_pipelines(self, filter=None):
        return iter(self._pipelines)

    def start_update(self, pipeline_id, development=None):
        self.started.append((pipeline_id, development))
        return SimpleNamespace(update_id="upd-1")


def client_with(*names):
    api = FakePipelinesAPI([SimpleNamespace(name=n, pipeline_id=f"id-{n}") for n in names])
    return SimpleNamespace(pipelines=api), api


def test_starts_a_development_update_on_the_exact_name_match():
    client, api = client_with("rearc_quest_pipeline", "rearc_quest_pipeline_old")
    assert SdkPipelineTrigger(client, "rearc_quest_pipeline", development=True).trigger() == "upd-1"
    assert api.started == [("id-rearc_quest_pipeline", True)]


@pytest.mark.parametrize("names", [(), ("rearc_quest_pipeline", "rearc_quest_pipeline")])
def test_refuses_unless_exactly_one_pipeline_matches(names):
    client, api = client_with(*names)
    with pytest.raises(RuntimeError, match="exactly one pipeline"):
        SdkPipelineTrigger(client, "rearc_quest_pipeline", development=True).trigger()
    assert api.started == []
