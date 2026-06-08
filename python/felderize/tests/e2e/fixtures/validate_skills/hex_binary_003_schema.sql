-- rule: hex_binary
-- spark: hex(x) — hex-encode a BINARY value; Spark also accepts integer/string but Feldera does not
-- feldera: UPPER(TO_HEX(col)) — Feldera TO_HEX returns lowercase; Spark hex() returns uppercase; ALWAYS wrap with UPPER(). Feldera TO_HEX accepts VARBINARY input only.
CREATE TABLE checksums (
  checksum_id INT,
  hash_value BINARY,
  file_name STRING
);
