-- rule: current_date
-- spark: CURRENT_DATE — today's date
-- feldera: CAST(NOW() AS DATE)
CREATE OR REPLACE TEMP VIEW events_summary_v1 AS SELECT
  event_id,
  event_name,
  event_timestamp,
  CURRENT_DATE AS report_date
FROM events_log
WHERE event_timestamp >= CAST(CURRENT_DATE AS TIMESTAMP);
