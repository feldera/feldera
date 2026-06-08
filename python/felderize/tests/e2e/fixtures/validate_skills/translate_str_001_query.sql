-- rule: translate_str
-- spark: translate(s, from_chars, to_chars) — replace each char in from with corresponding char in to
-- feldera: Chain of REPLACE calls: translate(s, 'aei', '123') → REPLACE(REPLACE(REPLACE(s, 'a', '1'), 'e', '2'), 'i', '3')
CREATE OR REPLACE TEMP VIEW translate_str_001_view AS SELECT id, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name, 'a', '1'), 'e', '2'), 'i', '3'), 'o', '4'), 'u', '5') AS translated_name FROM product_names;
