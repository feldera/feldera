-- rule: date_trunc_year
-- spark: date_trunc('YEAR', ts) — truncate timestamp to year
-- feldera: TIMESTAMP_TRUNC(ts, YEAR)
CREATE TABLE events_log (
  event_id INT,
  event_name STRING,
  event_time TIMESTAMP
);
