-- rule: split_regex
-- spark: split(str, '[1-9]+') or split(str, '\\s+') — split using a regex pattern
-- feldera: UNSUPPORTED — Feldera SPLIT treats delimiter as a literal string, not a regex. Regex patterns like '[1-9]+' or '\\s+' will not match as intended — mark unsupported.
CREATE OR REPLACE TEMP VIEW split_regex_v1 AS SELECT id, split(code_str, '[0-9]+') AS code_parts FROM product_codes;
