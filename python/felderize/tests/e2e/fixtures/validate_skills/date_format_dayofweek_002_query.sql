-- rule: date_format_dayofweek
-- spark: date_format(d, 'E') — extract abbreviated day-of-week name from a DATE (e.g. 'Mon', 'Tue', 'Wed')
-- feldera: FORMAT_DATE('%a', d) — Rust strftime %a gives the same 3-letter English abbreviation as Spark's 'E' format
CREATE OR REPLACE TEMP VIEW task_view AS SELECT task_id, scheduled_date, date_format(scheduled_date, 'E') AS weekday, task_priority FROM task_schedule WHERE task_priority > 0;
