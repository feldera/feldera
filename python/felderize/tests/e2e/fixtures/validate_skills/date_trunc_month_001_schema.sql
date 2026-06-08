-- rule: date_trunc_month
-- spark: date_trunc('MONTH', ts) — truncate timestamp to month
-- feldera: TIMESTAMP_TRUNC(ts, MONTH)  or  DATE_TRUNC(d, MONTH) for DATE input
CREATE TABLE events_log (
  event_id INT,
  event_name STRING,
  event_time TIMESTAMP
);
