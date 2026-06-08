-- rule: bit_or_agg
-- spark: bit_or(col) — bitwise OR aggregate over all rows in group
-- feldera: BIT_OR(col)
CREATE TABLE permissions_v2 (user_id INT, perm_mask LONG, dept STRING);
