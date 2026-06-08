-- rule: try_to_timestamp
-- spark: try_to_timestamp(str, fmt) — parse string as timestamp using Java format; returns NULL on failure instead of throwing
-- feldera: PARSE_TIMESTAMP(strptime_fmt, str) — translate Java format to strptime (e.g. 'yyyyMMdd' → '%Y%m%d'); Feldera raises a parse error on bad input instead of returning NULL
CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT
  event_id,
  event_time_str,
  try_to_timestamp(event_time_str, 'yyyy-MM-dd HH:mm:ss') AS parsed_timestamp
FROM event_logs;
