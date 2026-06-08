-- rule: ceil_floor_type
-- spark: CEIL(x) / FLOOR(x) on DOUBLE — Spark returns BIGINT; Feldera returns DOUBLE
-- feldera: CEIL(x) / FLOOR(x) — same function, add warning about return type difference
CREATE TABLE measurement_values (sensor_id INT, reading DOUBLE);
