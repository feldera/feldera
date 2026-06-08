-- rule: datediff
-- spark: datediff(end_date, start_date) — days between dates; Spark: 2 args, end first
-- feldera: DATEDIFF(DAY, start_date, end_date) — Feldera: 3 args, unit first, start before end
CREATE TABLE task_tracking (
  task_id INT,
  task_name STRING,
  assigned_date DATE,
  completed_date DATE
);
