-- rule: cast_numeric_ts
-- spark: CAST(numeric_expr AS TIMESTAMP) — cast a numeric value to TIMESTAMP; Spark interprets as seconds since epoch
-- feldera: CAST(numeric_expr AS TIMESTAMP) — pass through unchanged; Feldera compiler accepts this syntax
CREATE TABLE large_epoch_v3 (idx INT, seconds_since_epoch BIGINT, label STRING);
