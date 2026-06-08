-- rule: log2
-- spark: log2(x) — base-2 logarithm
-- feldera: LOG(x, 2)
CREATE TABLE bandwidth_values (
  channel_id STRING,
  bandwidth_mbps DOUBLE
);
