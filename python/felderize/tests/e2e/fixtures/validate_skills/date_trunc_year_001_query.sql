-- rule: date_trunc_year
-- spark: date_trunc('YEAR', ts) — truncate timestamp to year
-- feldera: TIMESTAMP_TRUNC(ts, YEAR)
CREATE OR REPLACE TEMP VIEW truncated_events_v1 AS SELECT event_id, event_name, date_trunc('YEAR', event_time) AS year_start FROM events_log;
