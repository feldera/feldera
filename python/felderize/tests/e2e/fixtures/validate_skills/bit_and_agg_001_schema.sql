-- rule: bit_and_agg
-- spark: bit_and(col) — bitwise AND aggregate over all rows in group
-- feldera: BIT_AND(col)
CREATE TABLE sensor_flags (sensor_id INT, flag_value INT);
CREATE TABLE sensor_readings (sensor_id INT, reading_value INT);
