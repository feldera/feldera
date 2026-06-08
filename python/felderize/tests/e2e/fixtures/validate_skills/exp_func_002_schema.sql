-- rule: exp_func
-- spark: EXP(x) — Euler's number e raised to the power x; input DOUBLE, output DOUBLE
-- feldera: EXP(x) — same function, supported directly in Feldera. Note [GBD-FP-PREC]: last-digit floating point differences between JVM and Rust may cause minor result differences.
CREATE TABLE exponential_log_b (record_id INT, power_input DOUBLE, category STRING);
