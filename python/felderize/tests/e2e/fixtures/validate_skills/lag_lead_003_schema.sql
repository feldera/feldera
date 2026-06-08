-- rule: lag_lead
-- spark: LAG(expr, offset, default) / LEAD(expr, offset, default) — access previous/next row
-- feldera: LAG / LEAD — same syntax
CREATE TABLE event_log (event_id INT, user_id INT, event_timestamp TIMESTAMP, event_type STRING);
