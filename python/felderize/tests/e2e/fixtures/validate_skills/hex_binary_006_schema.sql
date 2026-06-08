-- rule: hex_binary_substring
-- spark: substring(binary_col, pos, len) — slice a BINARY value
-- feldera: SUBSTRING(binary_col FROM pos FOR len)
CREATE TABLE binary_packets (pkt_id INT, payload BINARY);
