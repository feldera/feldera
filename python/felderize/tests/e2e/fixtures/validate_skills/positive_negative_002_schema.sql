-- rule: positive_negative
-- spark: positive(x) — unary no-op; negative(x) — unary negation
-- feldera: positive(x) → x; negative(x) → -x
CREATE TABLE temperature_log (
  sensor_id INT,
  reading_celsius INT,
  timestamp TIMESTAMP
);
