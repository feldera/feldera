-- rule: hex_binary_contains
-- spark: contains(binary_col, x'...') — check if binary value contains pattern
-- feldera: POSITION(x'...' IN binary_col) > 0
CREATE OR REPLACE TEMP VIEW hex_binary_view_005 AS SELECT msg_id, POSITION(x'6c6c' IN content) > 0 AS has_ll FROM binary_messages ORDER BY msg_id;
