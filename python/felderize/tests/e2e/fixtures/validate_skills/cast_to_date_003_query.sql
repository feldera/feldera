-- rule: cast_to_date
-- spark: CAST(string AS DATE) — parse ISO date string
-- feldera: CAST(string AS DATE) — pass through unchanged
CREATE OR REPLACE TEMP VIEW activity_dates AS SELECT
  user_id,
  CAST(activity_date_text AS DATE) AS activity_date,
  activity_type,
  duration_mins
FROM user_activities;
