-- rule: ceil_floor_type
-- spark: CEIL(x) / FLOOR(x) on DOUBLE — Spark returns BIGINT; Feldera returns DOUBLE
-- feldera: CEIL(x) / FLOOR(x) — same function, add warning about return type difference
CREATE OR REPLACE TEMP VIEW measurement_rounded AS SELECT sensor_id, CEIL(reading) AS rounded_up, FLOOR(reading) AS rounded_down FROM measurement_values;
