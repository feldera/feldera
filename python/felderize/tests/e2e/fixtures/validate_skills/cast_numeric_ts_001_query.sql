-- rule: cast_numeric_ts
-- spark: CAST(numeric_expr AS TIMESTAMP) — cast a numeric value to TIMESTAMP; Spark interprets as seconds since epoch
-- feldera: CAST(numeric_expr AS TIMESTAMP) — pass through unchanged; Feldera compiler accepts this syntax
CREATE OR REPLACE TEMP VIEW ts_conversion_v1 AS SELECT id, CAST(unix_time AS TIMESTAMP) AS ts_value, description FROM epoch_seconds_v1;
