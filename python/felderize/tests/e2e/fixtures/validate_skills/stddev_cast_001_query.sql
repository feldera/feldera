-- rule: stddev_cast
-- spark: STDDEV_SAMP(decimal_col) / STDDEV_POP(decimal_col) — Spark always returns DOUBLE; Feldera preserves input type
-- feldera: STDDEV_SAMP(CAST(decimal_col AS DOUBLE)) — cast non-DOUBLE input to DOUBLE to match Spark
CREATE OR REPLACE TEMP VIEW sales_stddev_v1 AS SELECT region, STDDEV_SAMP(amount) as sample_stddev, STDDEV_POP(amount) as pop_stddev FROM sales_metrics GROUP BY region;
