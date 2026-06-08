-- rule: from_unixtime_nofmt
-- spark: from_unixtime(n) — convert Unix epoch seconds (integer) to a TIMESTAMP string
-- feldera: TIMESTAMPADD(SECOND, n, DATE '1970-01-01') — returns TIMESTAMP; Feldera treats as UTC
CREATE OR REPLACE TEMP VIEW job_timeline_v3 AS SELECT
  job_id,
  job_name,
  from_unixtime(start_epoch) AS start_time,
  from_unixtime(end_epoch) AS end_time
FROM job_executions;
