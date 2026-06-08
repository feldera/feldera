-- rule: rpad_2arg
-- spark: RPAD(s, n) — 2-arg form: right-pad to width n using space as default pad character
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(' ', n-LENGTH(s))) END — use space literal in REPEAT since Feldera has no 2-arg RPAD
CREATE OR REPLACE TEMP VIEW padded_codes_v1 AS SELECT
  code_id,
  code,
  target_width,
  RPAD(code, target_width) AS padded_code
FROM product_codes;
