-- rule: bit_xor_agg
-- spark: bit_xor(col) — bitwise XOR aggregate over all rows in group
-- feldera: BIT_XOR(col)
CREATE OR REPLACE TEMP VIEW network_xor_v1 AS SELECT device_id, bit_xor(flag_value) AS xor_result FROM network_flags GROUP BY device_id;
