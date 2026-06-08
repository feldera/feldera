-- rule: current_date
-- spark: CURRENT_DATE — today's date
-- feldera: CAST(NOW() AS DATE)
CREATE TABLE events_log (
  event_id INT,
  event_name STRING,
  event_timestamp TIMESTAMP
);
