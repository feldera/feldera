-- rule: day_dayofmonth
-- spark: DAY(d) / day(d) — extract day-of-month from date or timestamp
-- feldera: DAYOFMONTH(d)
CREATE OR REPLACE TEMP VIEW day_extraction_v3 AS SELECT schedule_id, activity_date, day(activity_date) AS day_num, activity_type, priority FROM scheduling_data WHERE day(activity_date) >= 15;
