-- rule: rpad_2arg
-- spark: RPAD(s, n) — 2-arg form: right-pad to width n using space as default pad character
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(' ', n-LENGTH(s))) END — use space literal in REPEAT since Feldera has no 2-arg RPAD
CREATE TABLE order_ids (
  order_num INT,
  id_string STRING,
  width INT
);
