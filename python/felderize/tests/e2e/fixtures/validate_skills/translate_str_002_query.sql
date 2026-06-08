-- rule: translate_str
-- spark: translate(s, from_chars, to_chars) — replace each char in from with corresponding char in to
-- feldera: Chain of REPLACE calls: translate(s, 'aei', '123') → REPLACE(REPLACE(REPLACE(s, 'a', '1'), 'e', '2'), 'i', '3')
CREATE OR REPLACE TEMP VIEW translate_str_002_view AS SELECT user_id, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(message, 'A', '1'), 'B', '2'), 'C', '3'), 'D', '4'), 'E', '5'), 'F', '6'), 'G', '7') AS coded_message FROM user_messages;
