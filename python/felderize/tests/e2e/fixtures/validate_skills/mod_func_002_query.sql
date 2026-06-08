-- rule: mod_func
-- spark: MOD(a, b) / a % b — modulo operator; sign follows dividend (truncation toward zero)
-- feldera: MOD(a, b) / a % b — same, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW inventory_remainder_v2 AS SELECT item_id, stock_qty, batch_size, MOD(stock_qty, batch_size) AS items_remaining, stock_qty % batch_size AS leftover FROM inventory_check WHERE stock_qty > 0;
