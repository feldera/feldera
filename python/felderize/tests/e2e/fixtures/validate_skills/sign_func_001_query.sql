-- rule: sign_func
-- spark: sign(x) — returns -1.0, 0.0, or 1.0 depending on the sign of x (DOUBLE input/output)
-- feldera: SIGN(x) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW price_trend_v1 AS SELECT id, product_name, price_change, sign(price_change) AS trend_direction FROM price_changes;
