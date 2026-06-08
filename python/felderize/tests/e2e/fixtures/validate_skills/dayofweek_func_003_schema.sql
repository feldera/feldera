-- rule: dayofweek_func
-- spark: DAYOFWEEK(d) — day of week as integer (1=Sunday, 2=Monday, ..., 7=Saturday)
-- feldera: DAYOFWEEK(d) — same function, supported directly in Feldera
CREATE TABLE activity_log (activity_id BIGINT, activity_timestamp TIMESTAMP, user_id INT);
