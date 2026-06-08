-- rule: ln_log10
-- spark: LN(x) — natural logarithm; LOG10(x) — base-10 logarithm; both valid for positive x
-- feldera: LN(x) / LOG10(x) — same functions, supported directly in Feldera. Note [GBD-LOG-DOMAIN]: on x=0 Feldera returns -Infinity (Spark returns NULL); on negative x Feldera panics (Spark returns NULL). Use positive-only test data.
CREATE OR REPLACE TEMP VIEW exponential_analysis AS SELECT record_id, sample_value, LN(sample_value) AS natural_log_val, LOG10(sample_value) AS log10_val, (LN(sample_value) / LOG10(sample_value)) AS log_ratio FROM exponential_data;
