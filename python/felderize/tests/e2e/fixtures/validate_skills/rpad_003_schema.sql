-- rule: rpad
-- spark: RPAD(s, n, pad) — right-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END
CREATE TABLE order_refs (
  order_id INT,
  ref_code STRING,
  pad_width INT
);
