-- rule: pmod
-- spark: pmod(a, b) — positive modulo, always non-negative
-- feldera: MOD(MOD(a, b) + b, b)
CREATE OR REPLACE TEMP VIEW pmod_result_v2 AS SELECT item_id, stock_count, batch_size, pmod(stock_count, batch_size) AS leftover_units FROM inventory;
