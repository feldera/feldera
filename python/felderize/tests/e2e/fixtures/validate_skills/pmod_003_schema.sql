-- rule: pmod
-- spark: pmod(a, b) — positive modulo, always non-negative
-- feldera: MOD(MOD(a, b) + b, b)
CREATE TABLE measurements (sensor_id INT, reading INT, threshold INT);
