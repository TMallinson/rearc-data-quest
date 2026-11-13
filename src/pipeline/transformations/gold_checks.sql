-- Gold-level checks and the gate (D13, D36).

-- Parity: SQL primary vs PySpark alternative, whole rows compared both ways (D9, D37).
CREATE OR REFRESH MATERIALIZED VIEW dq_gold_parity (
  CONSTRAINT sql_and_pyspark_identical
    EXPECT (only_in_sql = 0 AND only_in_pyspark = 0 AND sql_rows = pyspark_rows)
    ON VIOLATION FAIL UPDATE
)
COMMENT 'One row per question: rows in only one implementation (EXCEPT ALL both ways).'
AS SELECT 'q1' AS question,
  (SELECT count(*) FROM q1_candidate_sql) AS sql_rows,
  (SELECT count(*) FROM q1_candidate_pyspark) AS pyspark_rows,
  (SELECT count(*) FROM (SELECT * FROM q1_candidate_sql EXCEPT ALL SELECT * FROM q1_candidate_pyspark)) AS only_in_sql,
  (SELECT count(*) FROM (SELECT * FROM q1_candidate_pyspark EXCEPT ALL SELECT * FROM q1_candidate_sql)) AS only_in_pyspark
UNION ALL SELECT 'q2',
  (SELECT count(*) FROM q2_candidate_sql),
  (SELECT count(*) FROM q2_candidate_pyspark),
  (SELECT count(*) FROM (SELECT * FROM q2_candidate_sql EXCEPT ALL SELECT * FROM q2_candidate_pyspark)),
  (SELECT count(*) FROM (SELECT * FROM q2_candidate_pyspark EXCEPT ALL SELECT * FROM q2_candidate_sql))
UNION ALL SELECT 'q3',
  (SELECT count(*) FROM q3_candidate_sql),
  (SELECT count(*) FROM q3_candidate_pyspark),
  (SELECT count(*) FROM (SELECT * FROM q3_candidate_sql EXCEPT ALL SELECT * FROM q3_candidate_pyspark)),
  (SELECT count(*) FROM (SELECT * FROM q3_candidate_pyspark EXCEPT ALL SELECT * FROM q3_candidate_sql));


-- The gate (D36): one row that reads every FAIL-level check. Every published Gold table
-- reads this row, so a failing check stops the update before Gold changes:
-- readers keep the last good answers ("stale, but never wrong").
-- Warn-level checks (dq_current_in_alldata, dq_q2_coverage) are deliberately not read.
CREATE OR REFRESH MATERIALIZED VIEW dq_gate (
  CONSTRAINT silver_keys_unique    EXPECT (duplicate_keys = 0)   ON VIOLATION FAIL UPDATE,
  CONSTRAINT alldata_input_present EXPECT (alldata_rows > 0)     ON VIOLATION FAIL UPDATE,
  CONSTRAINT gold_parity           EXPECT (parity_failures = 0)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT q1_six_years          EXPECT (q1_years = 6 AND q1_first_year = 2013 AND q1_last_year = 2018)
                                                                 ON VIOLATION FAIL UPDATE,
  CONSTRAINT q2_one_row_per_series EXPECT (q2_rows = q2_series)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT q2_basis_valid        EXPECT (q2_invalid_basis = 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT q3_has_rows           EXPECT (q3_rows > 0)          ON VIOLATION FAIL UPDATE
)
COMMENT 'Gate for all published Gold tables: one row of fail-level check results (D36).'
AS SELECT
  -- Silver multi-row checks (they also fail on their own; the gate makes Gold depend on them).
  (SELECT count_if(rows <> 1) FROM dq_unique_keys)  AS duplicate_keys,
  (SELECT alldata_rows FROM dq_alldata_input)       AS alldata_rows,
  -- Gold output checks, on the SQL candidates that feed the published tables.
  (SELECT count_if(NOT (only_in_sql = 0 AND only_in_pyspark = 0 AND sql_rows = pyspark_rows))
     FROM dq_gold_parity)                                                  AS parity_failures,
  (SELECT years FROM q1_candidate_sql)                                     AS q1_years,
  (SELECT first_year FROM q1_candidate_sql)                                AS q1_first_year,
  (SELECT last_year FROM q1_candidate_sql)                                 AS q1_last_year,
  (SELECT count(*) FROM q2_candidate_sql)                                  AS q2_rows,
  (SELECT count(DISTINCT series_id) FROM q2_candidate_sql)                 AS q2_series,
  (SELECT count_if(basis IS NULL OR basis NOT IN ('quarterly sum', 'annual value'))
     FROM q2_candidate_sql)                                                AS q2_invalid_basis,
  (SELECT count(*) FROM q3_candidate_sql)                                  AS q3_rows,
  -- Read so the Silver tables' row-level fail expectations are upstream of the gate.
  (SELECT count(*) FROM silver_pr_data)                                    AS silver_data_rows,
  (SELECT count(*) FROM silver_pr_series)                                  AS silver_series_rows,
  (SELECT count(*) FROM silver_population)                                 AS silver_population_rows,
  (SELECT count(*) FROM silver_pr_period) + (SELECT count(*) FROM silver_pr_measure)
    + (SELECT count(*) FROM silver_pr_sector) + (SELECT count(*) FROM silver_pr_duration)
    + (SELECT count(*) FROM silver_pr_footnote)                            AS silver_lookup_rows;


-- Warn-level, after Gold (not gating): every series in Silver should have a best year.
-- A series with no complete year is legitimately absent from Q2 (D4); this makes it visible.
CREATE OR REFRESH MATERIALIZED VIEW dq_q2_coverage (
  CONSTRAINT series_has_complete_year EXPECT (in_gold)
)
COMMENT 'One row per series in silver_pr_data: does it appear in gold_q2_best_year? (D13, warn)'
AS SELECT s.series_id, g.series_id IS NOT NULL AS in_gold
FROM (SELECT DISTINCT series_id FROM silver_pr_data) s
LEFT JOIN gold_q2_best_year g ON s.series_id = g.series_id;
