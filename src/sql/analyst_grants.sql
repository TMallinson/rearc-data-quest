-- D38: read-only analyst access to the three published Gold tables, and nothing else.
--
-- Run by the bundle job `rearc_quest_analyst_grants` on the serverless SQL warehouse:
--   databricks bundle deploy && databricks bundle run rearc_quest_analyst_grants
-- Idempotent: granting a privilege that is already held changes nothing, so it can be
-- re-run at any time. tests/test_grants.py fails if this file grants anything outside
-- the allowed set below.
--
-- Deliberately NOT granted:
--   SELECT on the schema: it would cover Bronze, Silver, the dq_* checks and every
--   future table. Volumes (raw, archive, state), Bronze, Silver and the checks.

GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.rearc_quest TO `analysts`;

GRANT SELECT ON TABLE workspace.rearc_quest.gold_q1_population_stats TO `analysts`;
GRANT SELECT ON TABLE workspace.rearc_quest.gold_q2_best_year TO `analysts`;
GRANT SELECT ON TABLE workspace.rearc_quest.gold_q3_series_population TO `analysts`;
