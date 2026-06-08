-- rule: bit_xor_agg
-- spark: bit_xor(col) — bitwise XOR aggregate over all rows in group
-- feldera: BIT_XOR(col)
CREATE OR REPLACE TEMP VIEW sensor_xor_v2 AS SELECT sensor_id, bit_xor(reading_code) AS xor_code FROM sensor_readings GROUP BY sensor_id;
