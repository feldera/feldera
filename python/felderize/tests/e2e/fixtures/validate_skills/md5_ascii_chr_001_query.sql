-- rule: md5_ascii_chr
-- spark: MD5(s) — MD5 hex digest of string; ASCII(s) — ASCII code of first character; CHR(n) — character from ASCII code
-- feldera: MD5(s) / ASCII(s) / CHR(n) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW product_hash_v1 AS SELECT id, code, MD5(code) AS code_hash, ASCII(code) AS first_ascii, CHR(65) AS sample_chr FROM product_codes;
