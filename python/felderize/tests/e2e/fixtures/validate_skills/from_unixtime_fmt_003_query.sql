-- rule: from_unixtime_fmt
-- spark: from_unixtime(n, 'yyyy-MM-dd HH:mm:ss') — Unix epoch seconds → formatted string using Java pattern
-- feldera: FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMPADD(SECOND, n, DATE '1970-01-01')) — translate Java fmt to strftime
CREATE OR REPLACE TEMP VIEW metrics_formatted AS
SELECT
  metric_id,
  metric_name,
  cpu_usage,
  from_unixtime(timestamp_unix, 'yyyy-MM-dd HH:mm:ss') AS recorded_at
FROM server_metrics
WHERE cpu_usage > 10.0;
