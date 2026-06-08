-- rule: stddev_cast
-- spark: STDDEV_SAMP(decimal_col) / STDDEV_POP(decimal_col) — Spark always returns DOUBLE; Feldera preserves input type
-- feldera: STDDEV_SAMP(CAST(decimal_col AS DOUBLE)) — cast non-DOUBLE input to DOUBLE to match Spark
CREATE TABLE price_points (
  product_id INT,
  price DECIMAL(8,3),
  category STRING
);
