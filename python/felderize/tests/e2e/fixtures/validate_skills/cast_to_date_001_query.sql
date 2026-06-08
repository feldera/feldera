-- rule: cast_to_date
-- spark: CAST(string AS DATE) — parse ISO date string
-- feldera: CAST(string AS DATE) — pass through unchanged
CREATE OR REPLACE TEMP VIEW events_parsed AS SELECT
  event_id,
  CAST(event_date_str AS DATE) AS parsed_date,
  event_name
FROM events_log;
