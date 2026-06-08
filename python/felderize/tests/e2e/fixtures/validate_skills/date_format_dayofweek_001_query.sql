-- rule: date_format_dayofweek
-- spark: date_format(d, 'E') — extract abbreviated day-of-week name from a DATE (e.g. 'Mon', 'Tue', 'Wed')
-- feldera: FORMAT_DATE('%a', d) — Rust strftime %a gives the same 3-letter English abbreviation as Spark's 'E' format
CREATE OR REPLACE TEMP VIEW events_view AS SELECT event_id, event_date, date_format(event_date, 'E') AS day_abbrev, event_name FROM events_log;
