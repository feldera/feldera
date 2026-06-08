-- rule: quarter_func
-- spark: quarter(d) — extract quarter number (1-4) from a DATE or TIMESTAMP
-- feldera: QUARTER(d) — same function name, supported directly in Feldera
CREATE TABLE event_logs (event_id INT, event_timestamp TIMESTAMP, event_type STRING);
