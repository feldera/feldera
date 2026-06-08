-- rule: to_date_func
-- spark: to_date(ts) / to_date(ts, fmt) — convert to DATE
-- feldera: CAST(ts AS DATE) — format parameter is ignored
CREATE TABLE event_log (event_id INT, event_time TIMESTAMP, event_name STRING);
