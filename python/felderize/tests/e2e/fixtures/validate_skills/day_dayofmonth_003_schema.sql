-- rule: day_dayofmonth
-- spark: DAY(d) / day(d) — extract day-of-month from date or timestamp
-- feldera: DAYOFMONTH(d)
CREATE TABLE scheduling_data (schedule_id INT, activity_date DATE, activity_type STRING, priority INT);
