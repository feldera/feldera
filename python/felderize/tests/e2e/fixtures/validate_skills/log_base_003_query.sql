-- rule: log_base
-- spark: log(base, value) — logarithm of value in given base; Spark: first arg=base
-- feldera: LOG(value, base) — CRITICAL: arg order is reversed. LOG(a, b) in Spark → LOG(b, a) in Feldera
CREATE OR REPLACE TEMP VIEW log_scientific_v3 AS SELECT
  calc_id,
  exponent_base,
  number,
  log(exponent_base, CAST(number AS DOUBLE)) AS logarithm
FROM scientific_calc
WHERE number > 0;
