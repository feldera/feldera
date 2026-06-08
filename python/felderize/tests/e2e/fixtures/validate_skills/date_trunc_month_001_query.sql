-- rule: date_trunc_month
-- spark: date_trunc('MONTH', ts) — truncate timestamp to month
-- feldera: TIMESTAMP_TRUNC(ts, MONTH)  or  DATE_TRUNC(d, MONTH) for DATE input
CREATE OR REPLACE TEMP VIEW events_monthly AS SELECT
  event_id,
  event_name,
  date_trunc('MONTH', event_time) AS month_start
FROM events_log
ORDER BY month_start, event_id;
