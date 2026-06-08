-- rule: log_base
-- spark: log(base, value) — logarithm of value in given base; Spark: first arg=base
-- feldera: LOG(value, base) — CRITICAL: arg order is reversed. LOG(a, b) in Spark → LOG(b, a) in Feldera
CREATE TABLE growth_data (
  event_id INT,
  logarithm_base DOUBLE,
  growth_factor DOUBLE
);
