-- rule: to_date_func
-- spark: to_date(ts) / to_date(ts, fmt) — convert to DATE
-- feldera: CAST(ts AS DATE) — format parameter is ignored
CREATE OR REPLACE TEMP VIEW event_dates_v1 AS SELECT event_id, to_date(event_time) AS event_date, event_name FROM event_log;
