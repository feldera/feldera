-- rule: bit_or_agg
-- spark: bit_or(col) — bitwise OR aggregate over all rows in group
-- feldera: BIT_OR(col)
CREATE TABLE status_bits_v3 (event_id INT, status_flag INT, region STRING, event_date DATE);
