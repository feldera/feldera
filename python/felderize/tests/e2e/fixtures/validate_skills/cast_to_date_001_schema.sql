-- rule: cast_to_date
-- spark: CAST(string AS DATE) — parse ISO date string
-- feldera: CAST(string AS DATE) — pass through unchanged
CREATE TABLE events_log (
  event_id INT,
  event_date_str STRING,
  event_name STRING
);
