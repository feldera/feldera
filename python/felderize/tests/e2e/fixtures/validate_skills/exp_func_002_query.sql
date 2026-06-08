-- rule: exp_func
-- spark: EXP(x) — Euler's number e raised to the power x; input DOUBLE, output DOUBLE
-- feldera: EXP(x) — same function, supported directly in Feldera. Note [GBD-FP-PREC]: last-digit floating point differences between JVM and Rust may cause minor result differences.
CREATE OR REPLACE TEMP VIEW exp_result_b AS SELECT record_id, power_input, category, EXP(power_input) AS computed_exp FROM exponential_log_b WHERE power_input IS NOT NULL;
