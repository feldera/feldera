-- rule: cast_to_timestamp
-- spark: CAST(string AS TIMESTAMP) — parse ISO timestamp string
-- feldera: CAST(string AS TIMESTAMP) — pass through unchanged
CREATE OR REPLACE TEMP VIEW sensor_timestamps_v2 AS SELECT sensor_id, CAST(timestamp_str AS TIMESTAMP) AS reading_time, temperature FROM sensor_data;
