-- rule: current_timestamp_now
-- spark: CURRENT_TIMESTAMP — current timestamp
-- feldera: NOW()
CREATE TABLE audit_trail_t2 (audit_id INT, action STRING, user_id INT, timestamp_action TIMESTAMP);
