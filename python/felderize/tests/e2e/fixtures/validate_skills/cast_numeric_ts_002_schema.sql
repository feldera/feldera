-- rule: cast_numeric_ts
-- spark: CAST(numeric_expr AS TIMESTAMP) — cast a numeric value to TIMESTAMP; Spark interprets as seconds since epoch
-- feldera: CAST(numeric_expr AS TIMESTAMP) — pass through unchanged; Feldera compiler accepts this syntax
CREATE TABLE numeric_timestamps_v2 (record_id INT, epoch_value INT, event_type STRING);
