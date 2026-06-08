-- rule: exp_func
-- spark: EXP(x) — Euler's number e raised to the power x; input DOUBLE, output DOUBLE
-- feldera: EXP(x) — same function, supported directly in Feldera. Note [GBD-FP-PREC]: last-digit floating point differences between JVM and Rust may cause minor result differences.
CREATE OR REPLACE TEMP VIEW exp_result_c AS SELECT measurement_id, exponent, observed_value, EXP(exponent) AS exponential_calc, EXP(exponent) * observed_value AS scaled_result FROM scientific_data_c WHERE observed_value > 0;
