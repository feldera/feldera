-- rule: log_base
-- spark: log(base, value) — logarithm of value in given base; Spark: first arg=base
-- feldera: LOG(value, base) — CRITICAL: arg order is reversed. LOG(a, b) in Spark → LOG(b, a) in Feldera
CREATE OR REPLACE TEMP VIEW log_growth_v2 AS SELECT
  event_id,
  logarithm_base,
  growth_factor,
  log(logarithm_base, growth_factor) AS log_growth
FROM growth_data
WHERE growth_factor > 0.0;
