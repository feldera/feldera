-- rule: datediff
-- spark: datediff(end_date, start_date) — days between dates; Spark: 2 args, end first
-- feldera: DATEDIFF(DAY, start_date, end_date) — Feldera: 3 args, unit first, start before end
CREATE OR REPLACE TEMP VIEW task_completion_v2 AS
SELECT
  task_id,
  task_name,
  assigned_date,
  completed_date,
  datediff(completed_date, assigned_date) AS days_to_complete
FROM task_tracking
WHERE completed_date IS NOT NULL;
