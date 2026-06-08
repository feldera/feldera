-- rule: pmod
-- spark: pmod(a, b) — positive modulo, always non-negative
-- feldera: MOD(MOD(a, b) + b, b)
CREATE TABLE inventory (item_id INT, stock_count INT, batch_size INT);
