-- rule: weekofyear
-- spark: weekofyear(d) — ISO week number of year
-- feldera: EXTRACT(WEEK FROM d)
CREATE TABLE task_schedule (task_id INT, scheduled_date DATE, priority STRING);
