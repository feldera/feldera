-- rule: cast_numeric_ts
-- spark: CAST(numeric_expr AS TIMESTAMP) — cast a numeric value to TIMESTAMP; Spark interprets as seconds since epoch
-- feldera: CAST(numeric_expr AS TIMESTAMP) — pass through unchanged; Feldera compiler accepts this syntax
CREATE OR REPLACE TEMP VIEW epoch_to_ts_v3 AS SELECT idx, CAST(seconds_since_epoch AS TIMESTAMP) AS converted_ts, label FROM large_epoch_v3;
