-- rule: cast_to_timestamp
-- spark: CAST(string AS TIMESTAMP) — parse ISO timestamp string
-- feldera: CAST(string AS TIMESTAMP) — pass through unchanged
CREATE TABLE event_logs (event_id INT, event_time_str VARCHAR(30), event_name VARCHAR(50)); CREATE TABLE processed_events (event_id INT, parsed_timestamp TIMESTAMP, event_name VARCHAR(50));
