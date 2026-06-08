-- rule: bit_or_agg
-- spark: bit_or(col) — bitwise OR aggregate over all rows in group
-- feldera: BIT_OR(col)
CREATE TABLE flags_log_v1 (id INT, flag_value INT, category STRING);
