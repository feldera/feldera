-- rule: translate_str
-- spark: translate(s, from_chars, to_chars) — replace each char in from with corresponding char in to
-- feldera: Chain of REPLACE calls: translate(s, 'aei', '123') → REPLACE(REPLACE(REPLACE(s, 'a', '1'), 'e', '2'), 'i', '3')
CREATE TABLE text_data (record_id INT, content VARCHAR);
