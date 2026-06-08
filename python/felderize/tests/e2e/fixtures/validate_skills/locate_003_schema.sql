-- rule: locate
-- spark: LOCATE(substr, str) — find 1-based position of substring
-- feldera: POSITION(substr IN str)
CREATE TABLE log_entries (
  log_id INT,
  message STRING,
  severity STRING
);
