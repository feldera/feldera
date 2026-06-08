-- rule: try_to_timestamp
-- spark: try_to_timestamp(str, fmt) — parse string as timestamp using Java format; returns NULL on failure instead of throwing
-- feldera: PARSE_TIMESTAMP(strptime_fmt, str) — translate Java format to strptime (e.g. 'yyyyMMdd' → '%Y%m%d'); Feldera raises a parse error on bad input instead of returning NULL
CREATE OR REPLACE TEMP VIEW audit_parsed AS SELECT
  record_id,
  timestamp_text,
  try_to_timestamp(timestamp_text, 'yyyyMMddHHmmss') AS parsed_ts,
  CASE WHEN try_to_timestamp(timestamp_text, 'yyyyMMddHHmmss') IS NULL THEN 'PARSE_FAILED' ELSE 'PARSE_OK' END AS parse_status
FROM audit_records;
