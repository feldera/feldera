-- rule: try_cast
-- spark: TRY_CAST(expr AS type) — cast that returns NULL on failure instead of raising an error
-- feldera: SAFE_CAST(expr AS type) — Feldera's exact equivalent; returns NULL on failure
CREATE TABLE sensor_data (reading_id INT, timestamp_str STRING, value_str STRING);
