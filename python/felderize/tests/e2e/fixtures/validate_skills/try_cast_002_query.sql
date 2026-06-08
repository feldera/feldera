-- rule: try_cast
-- spark: TRY_CAST(expr AS type) — cast that returns NULL on failure instead of raising an error
-- feldera: SAFE_CAST(expr AS type) — Feldera's exact equivalent; returns NULL on failure
CREATE OR REPLACE TEMP VIEW sensor_parsed AS SELECT reading_id, TRY_CAST(timestamp_str AS TIMESTAMP) AS event_time, TRY_CAST(value_str AS DOUBLE) AS numeric_value FROM sensor_data;
