-- rule: bit_and_agg
-- spark: bit_and(col) — bitwise AND aggregate over all rows in group
-- feldera: BIT_AND(col)
CREATE TABLE network_mask (device_id INT, mask_bits INT);
CREATE TABLE network_status (device_id INT, status INT);
