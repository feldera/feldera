-- rule: try_to_timestamp
-- spark: try_to_timestamp(str, fmt) — parse string as timestamp using Java format; returns NULL on failure instead of throwing
-- feldera: PARSE_TIMESTAMP(strptime_fmt, str) — translate Java format to strptime (e.g. 'yyyyMMdd' → '%Y%m%d'); Feldera raises a parse error on bad input instead of returning NULL
CREATE TABLE conversion_data (
  id INT,
  date_str STRING,
  time_str STRING,
  combined_str STRING
);
