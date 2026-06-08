-- rule: hex_binary_contains
-- spark: contains(binary_col, x'...') — check if binary value contains pattern
-- feldera: POSITION(x'...' IN binary_col) > 0
CREATE TABLE binary_messages (msg_id INT, content BINARY);
