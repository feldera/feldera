-- rule: hex_binary_length
-- spark: length(binary_col) — byte count of a BINARY value
-- feldera: OCTET_LENGTH(binary_col) — use OCTET_LENGTH, not LENGTH (LENGTH counts hex chars, not bytes)
CREATE TABLE binary_data (id INT, data BINARY);
