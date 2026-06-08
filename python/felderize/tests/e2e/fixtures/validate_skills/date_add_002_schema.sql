-- rule: date_add
-- spark: date_add(date, n) — add n days to a date
-- feldera: date + INTERVAL 'n' DAY  (literal n) or  date + n * INTERVAL '1' DAY  (column n)
CREATE TABLE task_schedule (task_id INT, start_date DATE, deadline_days INT); CREATE TABLE project_info (project_id INT, project_name STRING);
