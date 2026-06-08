-- rule: stddev_cast
-- spark: STDDEV_SAMP(decimal_col) / STDDEV_POP(decimal_col) — Spark always returns DOUBLE; Feldera preserves input type
-- feldera: STDDEV_SAMP(CAST(decimal_col AS DOUBLE)) — cast non-DOUBLE input to DOUBLE to match Spark
CREATE OR REPLACE TEMP VIEW revenue_stddev_v2 AS SELECT year, STDDEV_SAMP(revenue) as sample_std, STDDEV_POP(revenue) as population_std FROM revenue_data GROUP BY year;
