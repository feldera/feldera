-- rule: translate_str
-- spark: translate(s, from_chars, to_chars) — replace each char in from with corresponding char in to
-- feldera: Chain of REPLACE calls: translate(s, 'aei', '123') → REPLACE(REPLACE(REPLACE(s, 'a', '1'), 'e', '2'), 'i', '3')
CREATE OR REPLACE TEMP VIEW translate_str_003_view AS SELECT record_id, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(content, '0', 'a'), '1', 'b'), '2', 'c'), '3', 'd'), '4', 'e'), '5', 'f'), '6', 'g'), '7', 'h'), '8', 'i'), '9', 'j') AS obfuscated FROM text_data;
