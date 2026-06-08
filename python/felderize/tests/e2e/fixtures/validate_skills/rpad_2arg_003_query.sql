-- rule: rpad_2arg
-- spark: RPAD(s, n) — 2-arg form: right-pad to width n using space as default pad character
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(' ', n-LENGTH(s))) END — use space literal in REPEAT since Feldera has no 2-arg RPAD
CREATE OR REPLACE TEMP VIEW normalized_ids_v3 AS SELECT
  order_num,
  id_string,
  width,
  RPAD(id_string, width) AS normalized_id,
  LENGTH(RPAD(id_string, width)) AS result_length
FROM order_ids;
