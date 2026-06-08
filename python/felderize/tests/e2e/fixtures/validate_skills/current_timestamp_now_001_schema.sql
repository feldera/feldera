-- rule: current_timestamp_now
-- spark: CURRENT_TIMESTAMP — current timestamp
-- feldera: NOW()
CREATE TABLE event_log_t1 (event_id INT, event_name STRING, created_at TIMESTAMP);
