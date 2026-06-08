-- rule: datediff
-- spark: datediff(end_date, start_date) — days between dates; Spark: 2 args, end first
-- feldera: DATEDIFF(DAY, start_date, end_date) — Feldera: 3 args, unit first, start before end
CREATE OR REPLACE TEMP VIEW event_duration_v3 AS
SELECT
  event_id,
  event_type,
  launch_date,
  closure_date,
  datediff(closure_date, launch_date) AS event_span_days
FROM event_log
ORDER BY event_span_days DESC;
