-- rule: mod_func
-- spark: MOD(a, b) / a % b — modulo operator; sign follows dividend (truncation toward zero)
-- feldera: MOD(a, b) / a % b — same, supported directly in Feldera
CREATE TABLE inventory_check (item_id BIGINT, stock_qty BIGINT, batch_size BIGINT);
