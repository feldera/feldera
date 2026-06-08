-- rule: replace_regexp_replace
-- spark: REPLACE(s, search, replace) — literal string replacement; REGEXP_REPLACE(s, pattern, replace) — regex-based replacement
-- feldera: REPLACE(s, search, replace) / REGEXP_REPLACE(s, pattern, replace) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW normalized_text AS SELECT doc_id, REPLACE(content, '  ', ' ') AS cleaned_content, REGEXP_REPLACE(title, '[0-9]+', 'NUM') AS masked_title FROM text_content;
