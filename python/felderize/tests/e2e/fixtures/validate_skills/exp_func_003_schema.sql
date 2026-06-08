-- rule: exp_func
-- spark: EXP(x) — Euler's number e raised to the power x; input DOUBLE, output DOUBLE
-- feldera: EXP(x) — same function, supported directly in Feldera. Note [GBD-FP-PREC]: last-digit floating point differences between JVM and Rust may cause minor result differences.
CREATE TABLE scientific_data_c (measurement_id INT, exponent DOUBLE, observed_value INT);
