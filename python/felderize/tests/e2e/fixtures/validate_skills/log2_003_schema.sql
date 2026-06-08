-- rule: log2
-- spark: log2(x) — base-2 logarithm
-- feldera: LOG(x, 2)
CREATE TABLE data_sizes (
  file_id INT,
  size_bytes DOUBLE,
  file_name STRING
);
