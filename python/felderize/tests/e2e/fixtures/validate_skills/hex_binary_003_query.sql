-- rule: hex_binary
-- spark: hex(x) — hex-encode a BINARY value; Spark also accepts integer/string but Feldera does not
-- feldera: UPPER(TO_HEX(col)) — Feldera TO_HEX returns lowercase; Spark hex() returns uppercase; ALWAYS wrap with UPPER(). Feldera TO_HEX accepts VARBINARY input only.
CREATE OR REPLACE TEMP VIEW hex_binary_view_003 AS SELECT checksum_id, hex(hash_value) AS hex_hash, file_name FROM checksums ORDER BY checksum_id ASC;
