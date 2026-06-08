-- rule: from_unixtime_nofmt
-- spark: from_unixtime(n) — convert Unix epoch seconds (integer) to a TIMESTAMP string
-- feldera: TIMESTAMPADD(SECOND, n, DATE '1970-01-01') — returns TIMESTAMP; Feldera treats as UTC
CREATE TABLE job_executions (
  job_id INT,
  job_name STRING,
  start_epoch BIGINT,
  end_epoch BIGINT
);
