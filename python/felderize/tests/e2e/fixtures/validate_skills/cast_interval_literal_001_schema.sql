-- rule: cast_interval_literal
-- spark: CAST('3' AS INTERVAL DAY) / CAST('interval 3 day' AS interval day) — string to interval
-- feldera: INTERVAL '3' DAY — drop CAST, use interval literal directly
CREATE TABLE event_log (event_id INT, event_name STRING, created_at TIMESTAMP); CREATE TABLE event_schedule (schedule_id INT, task_name STRING, deadline TIMESTAMP);
