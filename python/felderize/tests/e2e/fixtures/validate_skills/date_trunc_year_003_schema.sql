-- rule: date_trunc_year
-- spark: date_trunc('YEAR', ts) — truncate timestamp to year
-- feldera: TIMESTAMP_TRUNC(ts, YEAR)
CREATE TABLE audit_trail (
  audit_id INT,
  action_type STRING,
  action_timestamp TIMESTAMP,
  user_name STRING
);
