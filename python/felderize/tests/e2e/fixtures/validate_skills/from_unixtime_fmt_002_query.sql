-- rule: from_unixtime_fmt
-- spark: from_unixtime(n, 'yyyy-MM-dd HH:mm:ss') — Unix epoch seconds → formatted string using Java pattern
-- feldera: FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMPADD(SECOND, n, DATE '1970-01-01')) — translate Java fmt to strftime
CREATE OR REPLACE TEMP VIEW request_times AS
SELECT
  request_id,
  endpoint,
  from_unixtime(response_time_unix, 'yyyy-MM-dd HH:mm:ss') AS response_timestamp
FROM api_requests;
