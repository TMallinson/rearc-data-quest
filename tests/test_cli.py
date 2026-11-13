"""The CLI must refuse to start on configuration errors before touching Databricks or the network."""
from rearc_ingest import cli
from rearc_ingest.config import Settings


def test_missing_user_agent_exits_2(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)  # no .env to find
    monkeypatch.delenv("BLS_USER_AGENT", raising=False)
    assert cli.main(["--no-refresh"]) == 2
    assert "BLS_USER_AGENT" in capsys.readouterr().err


def test_refresh_without_a_configured_pipeline_exits_2(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    unconfigured = Settings(bls_user_agent="T (t@example.com)", datausa_user_agent="x", pipeline_name=None)
    monkeypatch.setattr(cli, "_settings", lambda args: unconfigured)
    assert cli.main([]) == 2
    assert "--no-refresh" in capsys.readouterr().err
