-- rule: bit_xor_agg
-- spark: bit_xor(col) — bitwise XOR aggregate over all rows in group
-- feldera: BIT_XOR(col)
CREATE TABLE sensor_readings (sensor_id INT, reading_code INT); CREATE TABLE sensor_readings_result (sensor_id INT, xor_code INT);
