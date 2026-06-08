-- rule: abs_power_sqrt
-- spark: ABS(x) — absolute value; POWER(x, p) — exponentiation; SQRT(x) — square root
-- feldera: ABS(x) / POWER(x, p) / SQRT(x) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW price_calc_v1 AS SELECT product_id, ABS(price - 100) AS price_diff, POWER(discount, 2) AS discount_squared, SQRT(price) AS price_root FROM price_metrics WHERE price > 0;
