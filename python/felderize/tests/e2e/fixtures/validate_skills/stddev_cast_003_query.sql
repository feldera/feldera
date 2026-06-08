-- rule: stddev_cast
-- spark: STDDEV_SAMP(decimal_col) / STDDEV_POP(decimal_col) — Spark always returns DOUBLE; Feldera preserves input type
-- feldera: STDDEV_SAMP(CAST(decimal_col AS DOUBLE)) — cast non-DOUBLE input to DOUBLE to match Spark
CREATE OR REPLACE TEMP VIEW price_stddev_v3 AS SELECT category, STDDEV_SAMP(price) as sample_variance, STDDEV_POP(price) as total_variance FROM price_points GROUP BY category;
