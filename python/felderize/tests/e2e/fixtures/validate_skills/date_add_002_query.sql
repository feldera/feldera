-- rule: date_add
-- spark: date_add(date, n) — add n days to a date
-- feldera: date + INTERVAL 'n' DAY  (literal n) or  date + n * INTERVAL '1' DAY  (column n)
CREATE OR REPLACE TEMP VIEW task_deadlines_v2 AS SELECT task_id, start_date, deadline_days, date_add(start_date, deadline_days) AS due_date FROM task_schedule;
