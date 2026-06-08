-- rule: hex_binary_length
-- spark: length(binary_col) — byte count of a BINARY value
-- feldera: OCTET_LENGTH(binary_col) — use OCTET_LENGTH, not LENGTH (LENGTH counts hex chars, not bytes)
CREATE OR REPLACE TEMP VIEW hex_binary_view_004 AS SELECT id, OCTET_LENGTH(data) AS byte_len FROM binary_data ORDER BY id;
