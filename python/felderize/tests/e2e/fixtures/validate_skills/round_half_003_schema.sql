-- rule: round_half
-- spark: ROUND(x, d) — Spark rounds half-up (0.5→1); Feldera rounds half-to-even (0.5→0)
-- feldera: ROUND(x, d) — same function; add warning about rounding difference
CREATE TABLE invoice_items (invoice_id INT, item_price DECIMAL(12,4), quantity INT, discount_percent DECIMAL(5,2));
