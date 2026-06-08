-- rule: concat_pipe
-- spark: s || t — string concatenation using pipe operator (Spark supports this)
-- feldera: s || t — same operator, fully supported in Feldera; pass through as-is
CREATE OR REPLACE TEMP VIEW formatted_msgs_v2 AS SELECT msg_id, sender || ' -> ' || recipient || ': ' || content AS full_message FROM messages_2;
