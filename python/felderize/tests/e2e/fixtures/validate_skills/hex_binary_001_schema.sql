-- rule: hex_binary
-- spark: hex(x) — hex-encode a BINARY value; Spark also accepts integer/string but Feldera does not
-- feldera: UPPER(TO_HEX(col)) — Feldera TO_HEX returns lowercase; Spark hex() returns uppercase; ALWAYS wrap with UPPER(). Feldera TO_HEX accepts VARBINARY input only.
CREATE TABLE binary_logs (
  log_id INT,
  message_data BINARY,
  created_date DATE
);
