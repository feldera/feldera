-- rule: trim_func
-- spark: TRIM(s) — trim leading and trailing spaces from string
-- feldera: TRIM(s) — same function, works identically in Feldera; → [GBD-WHITESPACE]: only ASCII space (0x20) trimmed, not \t/\n
CREATE OR REPLACE TEMP VIEW trimmed_names_v1 AS SELECT id, TRIM(full_name) AS cleaned_name FROM user_names;
