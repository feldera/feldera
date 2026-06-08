-- rule: cast_to_date
-- spark: CAST(string AS DATE) — parse ISO date string
-- feldera: CAST(string AS DATE) — pass through unchanged
CREATE TABLE user_activities (
  user_id INT,
  activity_date_text STRING,
  activity_type STRING,
  duration_mins INT
);
