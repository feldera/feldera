-- rule: dayofweek_func
-- spark: DAYOFWEEK(d) — day of week as integer (1=Sunday, 2=Monday, ..., 7=Saturday)
-- feldera: DAYOFWEEK(d) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW event_day_analysis AS SELECT event_id, event_date, event_name, DAYOFWEEK(event_date) AS day_of_week FROM event_log;
