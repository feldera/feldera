-- rule: round_half
-- spark: ROUND(x, d) — Spark rounds half-up (0.5→1); Feldera rounds half-to-even (0.5→0)
-- feldera: ROUND(x, d) — same function; add warning about rounding difference
CREATE OR REPLACE TEMP VIEW rounded_prices_v1 AS SELECT product_id, ROUND(unit_price, 1) AS rounded_price, ROUND(tax_rate, 2) AS rounded_tax FROM price_data;
