-- rule: trim_func
-- spark: TRIM(s) — trim leading and trailing spaces from string
-- feldera: TRIM(s) — same function, works identically in Feldera; → [GBD-WHITESPACE]: only ASCII space (0x20) trimmed, not \t/\n
CREATE OR REPLACE TEMP VIEW clean_descriptions_v2 AS SELECT product_id, TRIM(description) AS trimmed_desc FROM product_descriptions WHERE LENGTH(TRIM(description)) > 0;
