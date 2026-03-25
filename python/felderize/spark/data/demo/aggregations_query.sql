CREATE OR REPLACE TEMP VIEW user_engagement AS
SELECT
  user_id,
  COUNT(DISTINCT page_url) AS unique_pages,
  COLLECT_LIST(page_url) AS visited_pages,
  AVG(view_duration) AS avg_duration,
  STDDEV(view_duration) AS stddev_duration,
  MIN(view_time) AS first_seen,
  MAX(view_time) AS last_seen,
  COUNT(CASE WHEN device_type = 'mobile' THEN 1 END) AS mobile_views
FROM page_views
GROUP BY user_id
HAVING COUNT(*) > 5;
