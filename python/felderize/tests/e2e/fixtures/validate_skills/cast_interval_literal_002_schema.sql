-- rule: cast_interval_literal
-- spark: CAST('3' AS INTERVAL DAY) / CAST('interval 3 day' AS interval day) — string to interval
-- feldera: INTERVAL '3' DAY — drop CAST, use interval literal directly
CREATE TABLE project_milestones (milestone_id INT, milestone_name STRING, start_date TIMESTAMP, duration_days INT); CREATE TABLE project_updates (update_id INT, milestone_id INT, update_text STRING, update_time TIMESTAMP);
