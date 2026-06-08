-- rule: date_format_dayofweek
-- spark: date_format(d, 'E') — extract abbreviated day-of-week name from a DATE (e.g. 'Mon', 'Tue', 'Wed')
-- feldera: FORMAT_DATE('%a', d) — Rust strftime %a gives the same 3-letter English abbreviation as Spark's 'E' format
CREATE TABLE task_schedule (
  task_id BIGINT,
  scheduled_date DATE,
  task_priority INT
);
