-- Gold answers, PRIMARY implementations in SQL (D37).
-- Private candidates: computed and checked here; published only through the gate (D36).
-- PySpark alternatives: gold_candidates_pyspark.py. Parity: gold_checks.sql.

-- Q1: mean and standard deviation of US population, 2013-2018 inclusive.
-- D2: population standard deviation is the answer; sample SD is a second, named column.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW q1_candidate_sql
COMMENT 'Q1 primary (SQL): population stats 2013-2018 (D2).'
AS SELECT
  count(*)                AS years,
  min(year)               AS first_year,
  max(year)               AS last_year,
  avg(population)         AS mean_population,
  stddev_pop(population)  AS stddev_population,
  stddev_samp(population) AS stddev_sample
FROM silver_population
WHERE year BETWEEN 2013 AND 2018;


-- Q2: for every series, the year with the largest total (D3, D4, D5, D17).
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW q2_candidate_sql
COMMENT 'Q2 primary (SQL): best year per series.'
AS WITH series_basis AS (
  -- D3: a series is annual-only if the data has no quarterly rows for it at all.
  SELECT series_id,
         CASE WHEN count_if(period IN ('Q01', 'Q02', 'Q03', 'Q04')) = 0
              THEN 'annual value' ELSE 'quarterly sum' END AS basis
  FROM silver_pr_data
  GROUP BY series_id
),
year_totals AS (
  -- Quarterly series: sum Q01-Q04, never Q05. Annual-only series: the Q05 value.
  SELECT d.series_id, b.basis, d.year,
         sum(CASE WHEN b.basis = 'quarterly sum' AND d.period IN ('Q01', 'Q02', 'Q03', 'Q04') THEN d.value
                  WHEN b.basis = 'annual value' AND d.period = 'Q05' THEN d.value END) AS total,
         count_if(d.period IN ('Q01', 'Q02', 'Q03', 'Q04')) AS quarter_count,
         count_if(d.period = 'Q05') AS annual_count
  FROM silver_pr_data d
  JOIN series_basis b ON d.series_id = b.series_id
  GROUP BY d.series_id, b.basis, d.year
),
complete_years AS (
  -- D4: rank only complete years, derived from the data (no hardcoded year).
  SELECT * FROM year_totals
  WHERE (basis = 'quarterly sum' AND quarter_count = 4)
     OR (basis = 'annual value' AND annual_count = 1)
),
best AS (
  SELECT series_id, max(total) AS best_total FROM complete_years GROUP BY series_id
),
tied AS (
  -- D17: every complete year that reaches the best total, sorted ascending.
  SELECT c.series_id, c.basis, c.total AS best_total,
         array_sort(collect_list(c.year)) AS tied_years,
         min_by(c.quarter_count, c.year) AS quarter_count
  FROM complete_years c
  JOIN best b ON c.series_id = b.series_id AND c.total = b.best_total
  GROUP BY c.series_id, c.basis, c.total
)
SELECT t.series_id, s.label, s.measure_text, s.sector_name, s.duration_text,  -- labels joined here (D35)
       t.basis,
       element_at(t.tied_years, 1)                    AS best_year,         -- D17: earliest tied year
       t.best_total,
       t.quarter_count,                                                     -- D4: 4 quarterly, 0 annual
       slice(t.tied_years, 2, size(t.tied_years) - 1) AS other_tied_years   -- D17: empty if no tie
FROM tied t
LEFT JOIN silver_pr_series s ON t.series_id = s.series_id;  -- LEFT: Q2 keeps series missing from pr.series


-- Q3: PRS30006032, period Q01, each year's value with that year's population (D15).
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW q3_candidate_sql
COMMENT 'Q3 primary (SQL): PRS30006032 Q01 by year, left-joined to population.'
AS SELECT d.series_id, d.year, d.period, d.value, p.population
FROM silver_pr_data d
LEFT JOIN silver_population p ON d.year = p.year  -- LEFT, INT = INT: years without population stay (D15)
WHERE d.series_id = 'PRS30006032' AND d.period = 'Q01';
