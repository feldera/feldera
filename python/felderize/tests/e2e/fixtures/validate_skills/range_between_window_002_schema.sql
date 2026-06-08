-- rule: range_between_window
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) — running sum with RANGE frame
-- feldera: Same — RANGE BETWEEN is fully supported in Feldera
CREATE TABLE temperature_log (sensor_id INT, reading_time TIMESTAMP, temp_celsius DECIMAL(5,2));
CREATE TABLE expected_temps (sensor_id INT, reading_time TIMESTAMP, temp_celsius DECIMAL(5,2), cumulative_temp DECIMAL(10,2));
