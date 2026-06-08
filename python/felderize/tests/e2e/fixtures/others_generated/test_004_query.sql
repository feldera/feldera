CREATE OR REPLACE TEMP VIEW bm04_engagement_by_channel AS
WITH web_activity AS (
  SELECT
    customer_id,
    region,
    session_start AS activity_time,
    duration_seconds,
    'WEB' AS channel
  FROM web_sessions
),
store_activity AS (
  SELECT
    customer_id,
    region,
    visit_start AS activity_time,
    duration_minutes * 60 AS duration_seconds,
    'STORE' AS channel
  FROM store_visits
),
all_activity AS (
  SELECT * FROM web_activity
  UNION ALL
  SELECT * FROM store_activity
)
SELECT
  region,
  channel,
  COUNT(*) AS activity_count,
  AVG(duration_seconds) AS avg_duration_seconds
FROM all_activity
GROUP BY region, channel;
