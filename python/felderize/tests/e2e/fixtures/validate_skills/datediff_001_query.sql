-- rule: datediff
-- spark: datediff(end_date, start_date) — days between dates; Spark: 2 args, end first
-- feldera: DATEDIFF(DAY, start_date, end_date) — Feldera: 3 args, unit first, start before end
CREATE OR REPLACE TEMP VIEW project_duration_v1 AS
SELECT
  project_id,
  project_name,
  start_date,
  end_date,
  datediff(end_date, start_date) AS days_duration
FROM project_timeline;
