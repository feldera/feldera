-- rule: ln_log10
-- spark: LN(x) — natural logarithm; LOG10(x) — base-10 logarithm; both valid for positive x
-- feldera: LN(x) / LOG10(x) — same functions, supported directly in Feldera. Note [GBD-LOG-DOMAIN]: on x=0 Feldera returns -Infinity (Spark returns NULL); on negative x Feldera panics (Spark returns NULL). Use positive-only test data.
CREATE OR REPLACE TEMP VIEW math_results_a AS SELECT id, value, LN(value) AS ln_result, LOG10(value) AS log10_result FROM math_values_a;
