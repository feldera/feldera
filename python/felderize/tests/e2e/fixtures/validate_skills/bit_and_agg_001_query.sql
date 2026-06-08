-- rule: bit_and_agg
-- spark: bit_and(col) — bitwise AND aggregate over all rows in group
-- feldera: BIT_AND(col)
CREATE OR REPLACE TEMP VIEW sensor_flag_agg AS SELECT sensor_id, bit_and(flag_value) AS combined_flags FROM sensor_flags GROUP BY sensor_id;
