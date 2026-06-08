-- rule: date_trunc_month
-- spark: date_trunc('MONTH', ts) — truncate timestamp to month
-- feldera: TIMESTAMP_TRUNC(ts, MONTH)  or  DATE_TRUNC(d, MONTH) for DATE input
CREATE TABLE audit_records (
  record_id INT,
  action STRING,
  timestamp_utc TIMESTAMP,
  status STRING
);
