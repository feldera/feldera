-- rule: to_date_func
-- spark: to_date(ts) / to_date(ts, fmt) — convert to DATE
-- feldera: CAST(ts AS DATE) — format parameter is ignored
CREATE OR REPLACE TEMP VIEW activity_by_date_v3 AS SELECT user_id, to_date(activity_ts) AS activity_date, activity_type, country FROM user_activity ORDER BY user_id;
