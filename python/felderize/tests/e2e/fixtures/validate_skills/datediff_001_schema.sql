-- rule: datediff
-- spark: datediff(end_date, start_date) — days between dates; Spark: 2 args, end first
-- feldera: DATEDIFF(DAY, start_date, end_date) — Feldera: 3 args, unit first, start before end
CREATE TABLE project_timeline (
  project_id INT,
  project_name STRING,
  start_date DATE,
  end_date DATE
);
