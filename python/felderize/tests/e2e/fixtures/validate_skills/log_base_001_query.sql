-- rule: log_base
-- spark: log(base, value) — logarithm of value in given base; Spark: first arg=base
-- feldera: LOG(value, base) — CRITICAL: arg order is reversed. LOG(a, b) in Spark → LOG(b, a) in Feldera
CREATE OR REPLACE TEMP VIEW log_results_v1 AS SELECT
  id,
  base_val,
  value_val,
  log(base_val, value_val) AS log_result
FROM measurements;
