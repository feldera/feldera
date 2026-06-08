-- rule: ln_log10
-- spark: LN(x) — natural logarithm; LOG10(x) — base-10 logarithm; both valid for positive x
-- feldera: LN(x) / LOG10(x) — same functions, supported directly in Feldera. Note [GBD-LOG-DOMAIN]: on x=0 Feldera returns -Infinity (Spark returns NULL); on negative x Feldera panics (Spark returns NULL). Use positive-only test data.
CREATE OR REPLACE TEMP VIEW logarithm_output AS SELECT test_id, measurement, ROUND(LN(measurement), 4) AS natural_log, ROUND(LOG10(measurement), 4) AS log_base_10 FROM logarithm_tests WHERE measurement > 0.1;
