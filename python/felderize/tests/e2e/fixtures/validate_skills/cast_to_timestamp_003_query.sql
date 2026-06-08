-- rule: cast_to_timestamp
-- spark: CAST(string AS TIMESTAMP) — parse ISO timestamp string
-- feldera: CAST(string AS TIMESTAMP) — pass through unchanged
CREATE OR REPLACE TEMP VIEW audit_timestamps_v3 AS SELECT record_id, CAST(action_timestamp_str AS TIMESTAMP) AS action_time, user_id, action_desc FROM audit_trail;
