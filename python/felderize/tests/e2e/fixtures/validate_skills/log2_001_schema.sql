-- rule: log2
-- spark: log2(x) — base-2 logarithm
-- feldera: LOG(x, 2)
CREATE TABLE signal_power (
  signal_id INT,
  power_level DOUBLE,
  timestamp TIMESTAMP
);
