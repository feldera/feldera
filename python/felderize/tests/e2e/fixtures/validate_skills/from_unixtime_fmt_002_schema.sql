-- rule: from_unixtime_fmt
-- spark: from_unixtime(n, 'yyyy-MM-dd HH:mm:ss') — Unix epoch seconds → formatted string using Java pattern
-- feldera: FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMPADD(SECOND, n, DATE '1970-01-01')) — translate Java fmt to strftime
CREATE TABLE api_requests (
  request_id INT,
  endpoint STRING,
  response_time_unix BIGINT
);
