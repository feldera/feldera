-- rule: rpad
-- spark: RPAD(s, n, pad) — right-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END
CREATE OR REPLACE TEMP VIEW formatted_usernames_v2 AS SELECT
  user_id,
  username,
  pad_char,
  desired_len,
  RPAD(username, desired_len, pad_char) AS formatted_name
FROM user_ids;
