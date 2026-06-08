-- rule: rpad
-- spark: RPAD(s, n, pad) — right-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END
CREATE OR REPLACE TEMP VIEW standardized_refs_v3 AS SELECT
  order_id,
  ref_code,
  pad_width,
  LENGTH(ref_code) AS original_length,
  LENGTH(RPAD(ref_code, pad_width, '0')) AS padded_length,
  RPAD(ref_code, pad_width, '0') AS normalized_ref
FROM order_refs;
