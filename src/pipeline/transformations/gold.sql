-- Published Gold tables: one per question, fed by the SQL primary (D37).
-- Each cross-joins the one-row gate. That join adds no rows or columns; it makes the
-- table depend on dq_gate, so a failing fail-level check stops the update before any
-- of these tables change (D36).

CREATE OR REFRESH MATERIALIZED VIEW gold_q1_population_stats
COMMENT 'Q1: US population 2013-2018. stddev_population is the answer (D2); stddev_sample is shown alongside.'
AS SELECT c.*
FROM q1_candidate_sql c
CROSS JOIN dq_gate g;

CREATE OR REFRESH MATERIALIZED VIEW gold_q2_best_year
COMMENT 'Q2: best year per series (largest quarterly sum, or annual value for annual-only series), complete years only; earliest year wins ties (D3, D4, D17).'
AS SELECT c.*
FROM q2_candidate_sql c
CROSS JOIN dq_gate g;

CREATE OR REFRESH MATERIALIZED VIEW gold_q3_series_population
COMMENT 'Q3: PRS30006032 Q01 value by year with US population where available; population is NULL for years DataUSA does not cover (2020 and outside 2013-2024).'
AS SELECT c.*
FROM q3_candidate_sql c
CROSS JOIN dq_gate g;
