-- rule: day_dayofmonth
-- spark: DAY(d) / day(d) — extract day-of-month from date or timestamp
-- feldera: DAYOFMONTH(d)
CREATE OR REPLACE TEMP VIEW day_extraction_v1 AS SELECT event_id, event_date, DAY(event_date) AS day_of_month, event_name FROM events_log;
