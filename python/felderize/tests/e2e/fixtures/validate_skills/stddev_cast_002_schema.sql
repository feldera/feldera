-- rule: stddev_cast
-- spark: STDDEV_SAMP(decimal_col) / STDDEV_POP(decimal_col) — Spark always returns DOUBLE; Feldera preserves input type
-- feldera: STDDEV_SAMP(CAST(decimal_col AS DOUBLE)) — cast non-DOUBLE input to DOUBLE to match Spark
CREATE TABLE revenue_data (
  trans_id INT,
  revenue DECIMAL(12,4),
  year INT
);
