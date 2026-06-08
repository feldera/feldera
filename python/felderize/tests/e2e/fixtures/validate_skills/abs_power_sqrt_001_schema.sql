-- rule: abs_power_sqrt
-- spark: ABS(x) — absolute value; POWER(x, p) — exponentiation; SQRT(x) — square root
-- feldera: ABS(x) / POWER(x, p) / SQRT(x) — all work identically in Feldera, no translation needed
CREATE TABLE price_metrics (product_id INT, price DOUBLE, discount DOUBLE);
