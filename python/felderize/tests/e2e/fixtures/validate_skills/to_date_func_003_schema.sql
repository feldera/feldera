-- rule: to_date_func
-- spark: to_date(ts) / to_date(ts, fmt) — convert to DATE
-- feldera: CAST(ts AS DATE) — format parameter is ignored
CREATE TABLE user_activity (user_id INT, activity_ts TIMESTAMP, activity_type STRING, country STRING);
