-- rule: last_day
-- spark: last_day(d) — last day of the month
-- feldera: DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY
CREATE OR REPLACE TEMP VIEW events_with_month_end AS SELECT
  event_id,
  event_date,
  last_day(event_date) AS month_end_date,
  description
FROM events_log;
