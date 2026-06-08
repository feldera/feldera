-- rule: exp_func
-- spark: EXP(x) — Euler's number e raised to the power x; input DOUBLE, output DOUBLE
-- feldera: EXP(x) — same function, supported directly in Feldera. Note [GBD-FP-PREC]: last-digit floating point differences between JVM and Rust may cause minor result differences.
CREATE OR REPLACE TEMP VIEW exp_result_a AS SELECT id, x_val, EXP(x_val) AS exp_result FROM math_values_a;
