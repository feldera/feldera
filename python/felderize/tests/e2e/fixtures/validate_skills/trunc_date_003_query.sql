-- rule: trunc_date
-- spark: trunc(d, 'YYYY'/'MM'/'QUARTER'/'WEEK') — truncate date using string unit; ONLY these formats work in Spark 4.x trunc() — do NOT use 'Q', 'DD', or 'DAY' as they return NULL
-- feldera: DATE_TRUNC(d, YEAR/MONTH/QUARTER/WEEK) — string unit becomes SQL keyword
CREATE OR REPLACE TEMP VIEW quarterly_milestones_v3 AS
SELECT
  trunc(milestone_date, 'QUARTER') AS quarter_start,
  COUNT(*) AS milestone_count,
  milestone_name
FROM project_milestones
GROUP BY trunc(milestone_date, 'QUARTER'), milestone_name
ORDER BY quarter_start, milestone_name;
