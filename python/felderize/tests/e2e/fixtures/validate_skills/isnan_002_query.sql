-- rule: isnan
-- spark: isnan(x) — true if x is IEEE 754 NaN
-- feldera: IS_NAN(x)
CREATE OR REPLACE TEMP VIEW metric_validation_v2 AS SELECT metric_id, isnan(value) AS value_is_nan, isnan(threshold) AS threshold_is_nan FROM metric_values;
