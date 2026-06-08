-- rule: weekofyear
-- spark: weekofyear(d) — ISO week number of year
-- feldera: EXTRACT(WEEK FROM d)
CREATE TABLE event_log (event_id INT, event_date DATE, event_name STRING);
