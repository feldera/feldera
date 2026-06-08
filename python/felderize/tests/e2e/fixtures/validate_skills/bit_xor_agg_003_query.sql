-- rule: bit_xor_agg
-- spark: bit_xor(col) — bitwise XOR aggregate over all rows in group
-- feldera: BIT_XOR(col)
CREATE OR REPLACE TEMP VIEW permission_xor_v3 AS SELECT user_group, bit_xor(permission) AS combined_permission FROM permission_bits GROUP BY user_group;
