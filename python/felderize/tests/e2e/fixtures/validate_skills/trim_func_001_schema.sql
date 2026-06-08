-- rule: trim_func
-- spark: TRIM(s) — trim leading and trailing spaces from string
-- feldera: TRIM(s) — same function, works identically in Feldera; → [GBD-WHITESPACE]: only ASCII space (0x20) trimmed, not \t/\n
CREATE TABLE user_names (id INT, full_name STRING);
