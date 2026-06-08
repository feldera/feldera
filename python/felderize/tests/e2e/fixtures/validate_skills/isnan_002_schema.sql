-- rule: isnan
-- spark: isnan(x) — true if x is IEEE 754 NaN
-- feldera: IS_NAN(x)
CREATE TABLE metric_values (metric_id STRING, value FLOAT, threshold FLOAT); CREATE TABLE validation_log (metric_id STRING, value_is_nan BOOLEAN, threshold_is_nan BOOLEAN);
