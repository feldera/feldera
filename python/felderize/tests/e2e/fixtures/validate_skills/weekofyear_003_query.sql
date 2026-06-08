-- rule: weekofyear
-- spark: weekofyear(d) — ISO week number of year
-- feldera: EXTRACT(WEEK FROM d)
CREATE OR REPLACE TEMP VIEW task_week_v3 AS SELECT task_id, scheduled_date, priority, weekofyear(scheduled_date) AS schedule_week FROM task_schedule;
