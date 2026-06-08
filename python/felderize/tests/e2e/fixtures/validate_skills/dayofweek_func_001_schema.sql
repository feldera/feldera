-- rule: dayofweek_func
-- spark: DAYOFWEEK(d) — day of week as integer (1=Sunday, 2=Monday, ..., 7=Saturday)
-- feldera: DAYOFWEEK(d) — same function, supported directly in Feldera
CREATE TABLE event_log (event_id INT, event_date DATE, event_name STRING);
