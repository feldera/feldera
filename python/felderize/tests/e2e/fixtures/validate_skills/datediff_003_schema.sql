-- rule: datediff
-- spark: datediff(end_date, start_date) — days between dates; Spark: 2 args, end first
-- feldera: DATEDIFF(DAY, start_date, end_date) — Feldera: 3 args, unit first, start before end
CREATE TABLE event_log (
  event_id INT,
  event_type STRING,
  launch_date DATE,
  closure_date DATE
);
