-- rule: bit_xor_agg
-- spark: bit_xor(col) — bitwise XOR aggregate over all rows in group
-- feldera: BIT_XOR(col)
CREATE TABLE network_flags (device_id INT, flag_value INT); CREATE TABLE network_flags_result (device_id INT, xor_result INT);
