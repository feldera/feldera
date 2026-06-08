-- rule: positive_negative
-- spark: positive(x) — unary no-op; negative(x) — unary negation
-- feldera: positive(x) → x; negative(x) → -x
CREATE OR REPLACE TEMP VIEW temp_transformed AS SELECT
  sensor_id,
  positive(reading_celsius) AS celsius_value,
  negative(reading_celsius) AS inverted_temp
FROM temperature_log;
