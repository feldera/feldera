-- rule: cast_numeric_ts
-- spark: CAST(numeric_expr AS TIMESTAMP) — cast a numeric value to TIMESTAMP; Spark interprets as seconds since epoch
-- feldera: CAST(numeric_expr AS TIMESTAMP) — pass through unchanged; Feldera compiler accepts this syntax
CREATE OR REPLACE TEMP VIEW timestamp_cast_v2 AS SELECT record_id, CAST(epoch_value AS TIMESTAMP) AS event_ts, event_type FROM numeric_timestamps_v2 WHERE epoch_value > 0;
