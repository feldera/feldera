-- rule: weekofyear
-- spark: weekofyear(d) — ISO week number of year
-- feldera: EXTRACT(WEEK FROM d)
CREATE OR REPLACE TEMP VIEW event_week_v1 AS SELECT event_id, event_date, event_name, weekofyear(event_date) AS iso_week FROM event_log;
