-- rule: range_between_window
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) — running sum with RANGE frame
-- feldera: Same — RANGE BETWEEN is fully supported in Feldera
CREATE OR REPLACE TEMP VIEW temp_accumulation_v2 AS SELECT sensor_id, reading_time, temp_celsius, SUM(temp_celsius) OVER (PARTITION BY sensor_id ORDER BY reading_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp FROM temperature_log;
