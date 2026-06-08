-- rule: hex_binary_substring
-- spark: substring(binary_col, pos, len) — slice a BINARY value
-- feldera: SUBSTRING(binary_col FROM pos FOR len)
CREATE OR REPLACE TEMP VIEW hex_binary_view_006 AS SELECT pkt_id, UPPER(TO_HEX(SUBSTRING(payload FROM 2 FOR 3))) AS sub_hex FROM binary_packets ORDER BY pkt_id;
