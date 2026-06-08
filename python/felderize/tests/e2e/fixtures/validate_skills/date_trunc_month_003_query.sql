-- rule: date_trunc_month
-- spark: date_trunc('MONTH', ts) — truncate timestamp to month
-- feldera: TIMESTAMP_TRUNC(ts, MONTH)  or  DATE_TRUNC(d, MONTH) for DATE input
CREATE OR REPLACE TEMP VIEW audit_by_month AS SELECT
  date_trunc('MONTH', timestamp_utc) AS month_bucket,
  action,
  COUNT(*) AS action_count
FROM audit_records
WHERE status = 'completed'
GROUP BY date_trunc('MONTH', timestamp_utc), action
ORDER BY month_bucket, action;
