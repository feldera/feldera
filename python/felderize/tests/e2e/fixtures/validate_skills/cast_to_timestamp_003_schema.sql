-- rule: cast_to_timestamp
-- spark: CAST(string AS TIMESTAMP) — parse ISO timestamp string
-- feldera: CAST(string AS TIMESTAMP) — pass through unchanged
CREATE TABLE audit_trail (record_id INT, action_timestamp_str VARCHAR(30), user_id INT, action_desc VARCHAR(100)); CREATE TABLE audit_processed (record_id INT, action_time TIMESTAMP, user_id INT, action_desc VARCHAR(100));
