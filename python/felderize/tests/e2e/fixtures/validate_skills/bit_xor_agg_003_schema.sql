-- rule: bit_xor_agg
-- spark: bit_xor(col) — bitwise XOR aggregate over all rows in group
-- feldera: BIT_XOR(col)
CREATE TABLE permission_bits (user_group STRING, permission INT); CREATE TABLE permission_bits_result (user_group STRING, combined_permission INT);
