-- rule: replace_regexp_replace
-- spark: REPLACE(s, search, replace) — literal string replacement; REGEXP_REPLACE(s, pattern, replace) — regex-based replacement
-- feldera: REPLACE(s, search, replace) / REGEXP_REPLACE(s, pattern, replace) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW standardized_phones AS SELECT contact_id, REGEXP_REPLACE(phone_number, '[^0-9]', '') AS digits_only, REPLACE(formatted_phone, '-', '/') AS slash_format FROM phone_records;
