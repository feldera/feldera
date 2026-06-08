-- rule: cast_interval_literal
-- spark: CAST('3' AS INTERVAL DAY) / CAST('interval 3 day' AS interval day) — string to interval
-- feldera: INTERVAL '3' DAY — drop CAST, use interval literal directly
CREATE OR REPLACE TEMP VIEW event_deadlines AS SELECT event_log.event_id, event_log.event_name, event_log.created_at, event_schedule.deadline, (event_schedule.deadline - CAST('5' AS INTERVAL DAY)) AS adjusted_deadline FROM event_log INNER JOIN event_schedule ON event_log.event_id = event_schedule.schedule_id;
