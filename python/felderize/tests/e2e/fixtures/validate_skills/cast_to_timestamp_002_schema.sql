-- rule: cast_to_timestamp
-- spark: CAST(string AS TIMESTAMP) — parse ISO timestamp string
-- feldera: CAST(string AS TIMESTAMP) — pass through unchanged
CREATE TABLE sensor_data (sensor_id INT, timestamp_str VARCHAR(30), temperature DECIMAL(5,2)); CREATE TABLE sensor_readings (sensor_id INT, reading_time TIMESTAMP, temperature DECIMAL(5,2));
