-- rule: from_unixtime_fmt
-- spark: from_unixtime(n, 'yyyy-MM-dd HH:mm:ss') — Unix epoch seconds → formatted string using Java pattern
-- feldera: FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMPADD(SECOND, n, DATE '1970-01-01')) — translate Java fmt to strftime
CREATE OR REPLACE TEMP VIEW formatted_events AS
SELECT
  event_id,
  event_name,
  from_unixtime(unix_timestamp, 'yyyy-MM-dd HH:mm:ss') AS event_time
FROM events_log;
