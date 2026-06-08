-- rule: round_half
-- spark: ROUND(x, d) — Spark rounds half-up (0.5→1); Feldera rounds half-to-even (0.5→0)
-- feldera: ROUND(x, d) — same function; add warning about rounding difference
CREATE OR REPLACE TEMP VIEW invoice_calculations_v3 AS SELECT invoice_id, ROUND(item_price, 2) AS price_rounded, ROUND(discount_percent, 1) AS discount_rounded FROM invoice_items;
