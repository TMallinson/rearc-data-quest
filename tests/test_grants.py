"""D38: the analyst grants file may grant only read access to the published Gold tables.

Fails if src/sql/analyst_grants.sql grants anything on a non-Gold object, grants SELECT at
schema or catalog level, grants any other privilege, or misses a Gold table.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
GRANTS_FILE = REPO / "src" / "sql" / "analyst_grants.sql"
GRANT = re.compile(
    r"GRANT\s+(?P<privilege>[A-Z ]+?)\s+ON\s+(?P<kind>CATALOG|SCHEMA|TABLE|VIEW|MATERIALIZED VIEW|VOLUME|FUNCTION)"
    r"\s+(?P<object>[\w.`]+)\s+TO\s+`(?P<principal>[^`]+)`",
    re.IGNORECASE,
)


def _statements(sql: str) -> list[str]:
    without_comments = re.sub(r"--[^\n]*", "", sql)
    return [" ".join(s.split()) for s in without_comments.split(";") if s.strip()]


def _bundle_names() -> tuple[str, str, set[str]]:
    variables = yaml.safe_load((REPO / "databricks.yml").read_text())["variables"]
    gold_sql = (REPO / "src" / "pipeline" / "transformations" / "gold.sql").read_text()
    gold = set(re.findall(r"CREATE OR REFRESH MATERIALIZED VIEW (\w+)", gold_sql))
    return variables["catalog"]["default"], variables["schema"]["default"], gold


def grant_problems(sql: str) -> list[str]:
    catalog, schema, gold_tables = _bundle_names()
    allowed = {("USE CATALOG", "CATALOG", catalog), ("USE SCHEMA", "SCHEMA", f"{catalog}.{schema}")}
    allowed |= {("SELECT", "TABLE", f"{catalog}.{schema}.{t}") for t in gold_tables}

    problems, granted = [], set()
    for statement in _statements(sql):
        match = GRANT.fullmatch(statement)
        if not match:
            problems.append(f"not a single-privilege GRANT to a group: {statement!r}")
            continue
        privilege = " ".join(match["privilege"].upper().split())
        kind = " ".join(match["kind"].upper().split())
        key = (privilege, kind, match["object"].replace("`", ""))
        if match["principal"] != "analysts":
            problems.append(f"grant to a principal other than analysts: {statement!r}")
        if privilege == "SELECT" and kind in ("SCHEMA", "CATALOG"):
            problems.append(f"SELECT at {kind.lower()} level: {statement!r}")
        elif key not in allowed:
            problems.append(f"grant outside the allowed set: {statement!r}")
        granted.add(key)
    problems += [f"missing grant: {' / '.join(k)}" for k in sorted(allowed - granted)]
    return problems


def test_grants_file_allows_read_only_gold_access_and_nothing_else():
    assert grant_problems(GRANTS_FILE.read_text()) == []


def test_gold_table_set_comes_from_the_pipeline():
    assert _bundle_names()[2] == {"gold_q1_population_stats", "gold_q2_best_year", "gold_q3_series_population"}


BASE = GRANTS_FILE.read_text()


def test_check_catches_schema_level_select():
    bad = BASE + "\nGRANT SELECT ON SCHEMA workspace.rearc_quest TO `analysts`;"
    assert any("SELECT at schema level" in p for p in grant_problems(bad))


def test_check_catches_grants_on_non_gold_objects():
    for extra in [
        "GRANT SELECT ON TABLE workspace.rearc_quest.silver_pr_data TO `analysts`;",
        "GRANT SELECT ON TABLE workspace.rearc_quest.bronze_pr_data TO `analysts`;",
        "GRANT SELECT ON TABLE workspace.rearc_quest.dq_gate TO `analysts`;",
        "GRANT READ VOLUME ON VOLUME workspace.rearc_quest.raw TO `analysts`;",
        "GRANT MODIFY ON TABLE workspace.rearc_quest.gold_q2_best_year TO `analysts`;",
        "GRANT ALL PRIVILEGES ON SCHEMA workspace.rearc_quest TO `analysts`;",
    ]:
        assert any("outside the allowed set" in p for p in grant_problems(BASE + "\n" + extra)), extra


def test_check_catches_a_missing_gold_grant():
    bad = BASE.replace("GRANT SELECT ON TABLE workspace.rearc_quest.gold_q3_series_population TO `analysts`;", "")
    assert grant_problems(bad) == ["missing grant: SELECT / TABLE / workspace.rearc_quest.gold_q3_series_population"]


def test_grants_job_runs_the_file_on_a_warehouse_without_cluster_settings():
    job_yaml = (REPO / "resources" / "rearc_quest.grants.job.yml").read_text()
    (job,) = yaml.safe_load(job_yaml)["resources"]["jobs"].values()
    (task,) = job["tasks"]
    assert task["sql_task"]["file"]["path"] == "../src/sql/analyst_grants.sql"
    assert task["sql_task"]["warehouse_id"] == "${var.warehouse_id}"
    assert not re.search(r"new_cluster|node_type_id|spark_version|job_cluster", job_yaml)
