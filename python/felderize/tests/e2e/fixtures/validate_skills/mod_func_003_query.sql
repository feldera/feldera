-- rule: mod_func
-- spark: MOD(a, b) / a % b — modulo operator; sign follows dividend (truncation toward zero)
-- feldera: MOD(a, b) / a % b — same, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW modulo_results_v3 AS SELECT calc_id, numerator, denominator, MOD(numerator, denominator) AS modulo_val FROM calculations WHERE denominator <> 0;
