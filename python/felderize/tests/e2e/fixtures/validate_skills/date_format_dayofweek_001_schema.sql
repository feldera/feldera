-- rule: date_format_dayofweek
-- spark: date_format(d, 'E') — extract abbreviated day-of-week name from a DATE (e.g. 'Mon', 'Tue', 'Wed')
-- feldera: FORMAT_DATE('%a', d) — Rust strftime %a gives the same 3-letter English abbreviation as Spark's 'E' format
CREATE TABLE events_log (
  event_id INT,
  event_date DATE,
  event_name STRING
);
