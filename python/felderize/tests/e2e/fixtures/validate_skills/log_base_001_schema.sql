-- rule: log_base
-- spark: log(base, value) — logarithm of value in given base; Spark: first arg=base
-- feldera: LOG(value, base) — CRITICAL: arg order is reversed. LOG(a, b) in Spark → LOG(b, a) in Feldera
CREATE TABLE measurements (
  id INT,
  base_val DOUBLE,
  value_val DOUBLE
);
