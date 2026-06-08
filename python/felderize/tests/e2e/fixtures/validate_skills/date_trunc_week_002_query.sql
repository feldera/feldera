-- rule: date_trunc_week
-- spark: date_trunc('WEEK', d) / trunc(d, 'WEEK') — truncate date to start of week; Spark uses Monday as first day of week
-- feldera: DATE_TRUNC(d - INTERVAL '1' DAY, WEEK) + INTERVAL '1' DAY — Feldera truncates to Sunday; subtract 1 day before truncating to handle all days correctly
CREATE OR REPLACE TEMP VIEW weekly_events_v2 AS SELECT event_name, event_date, trunc(event_date, 'WEEK') AS week_beginning FROM event_schedule WHERE event_date >= CAST('2024-02-05' AS DATE) ORDER BY event_date;
