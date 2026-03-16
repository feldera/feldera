CREATE TABLE page_views (
  view_id BIGINT,
  user_id BIGINT,
  page_url VARCHAR,
  referrer VARCHAR,
  device_type VARCHAR,
  view_duration INT,
  view_time TIMESTAMP
);

CREATE VIEW user_engagement AS
SELECT
  user_id,
  COUNT(DISTINCT page_url) AS unique_pages,
  AVG(view_duration) AS avg_duration,
  MIN(view_time) AS first_seen,
  MAX(view_time) AS last_seen,
  COUNT(CASE WHEN device_type = 'mobile' THEN 1 END) AS mobile_views
FROM page_views
GROUP BY user_id
HAVING COUNT(*) > 5;
