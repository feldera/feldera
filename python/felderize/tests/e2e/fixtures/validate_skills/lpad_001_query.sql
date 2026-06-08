-- rule: lpad
-- spark: LPAD(s, n, pad) — left-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END
CREATE OR REPLACE TEMP VIEW lpad_padded_codes AS SELECT id, LPAD(code, target_width, '0') AS padded_code FROM product_codes;
