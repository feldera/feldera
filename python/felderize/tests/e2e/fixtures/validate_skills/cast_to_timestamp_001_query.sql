-- rule: cast_to_timestamp
-- spark: CAST(string AS TIMESTAMP) — parse ISO timestamp string
-- feldera: CAST(string AS TIMESTAMP) — pass through unchanged
CREATE OR REPLACE TEMP VIEW event_timestamps_v1 AS SELECT event_id, CAST(event_time_str AS TIMESTAMP) AS parsed_ts, event_name FROM event_logs;
