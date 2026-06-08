-- rule: day_dayofmonth
-- spark: DAY(d) / day(d) — extract day-of-month from date or timestamp
-- feldera: DAYOFMONTH(d)
CREATE TABLE events_log (event_id INT, event_date DATE, event_name STRING);
