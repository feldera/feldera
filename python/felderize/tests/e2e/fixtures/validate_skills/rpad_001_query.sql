-- rule: rpad
-- spark: RPAD(s, n, pad) — right-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END
CREATE OR REPLACE TEMP VIEW padded_codes_v1 AS SELECT
  id,
  code,
  target_width,
  RPAD(code, target_width, '*') AS padded_code
FROM product_codes;
