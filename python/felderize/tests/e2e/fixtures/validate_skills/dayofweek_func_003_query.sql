-- rule: dayofweek_func
-- spark: DAYOFWEEK(d) — day of week as integer (1=Sunday, 2=Monday, ..., 7=Saturday)
-- feldera: DAYOFWEEK(d) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW activity_by_weekday AS SELECT activity_id, activity_timestamp, user_id, DAYOFWEEK(CAST(activity_timestamp AS DATE)) AS dow FROM activity_log ORDER BY DAYOFWEEK(CAST(activity_timestamp AS DATE));
