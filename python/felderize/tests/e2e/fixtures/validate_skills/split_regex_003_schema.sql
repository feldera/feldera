-- rule: split_regex
-- spark: split(str, '[1-9]+') or split(str, '\\s+') — split using a regex pattern
-- feldera: UNSUPPORTED — Feldera SPLIT treats delimiter as a literal string, not a regex. Regex patterns like '[1-9]+' or '\\s+' will not match as intended — mark unsupported.
CREATE TABLE phone_numbers (seq INT, phone_input STRING);
