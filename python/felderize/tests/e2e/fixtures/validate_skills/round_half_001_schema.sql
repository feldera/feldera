-- rule: round_half
-- spark: ROUND(x, d) — Spark rounds half-up (0.5→1); Feldera rounds half-to-even (0.5→0)
-- feldera: ROUND(x, d) — same function; add warning about rounding difference
CREATE TABLE price_data (product_id INT, unit_price DECIMAL(10,2), tax_rate DECIMAL(5,4));
