-- rule: rpad
-- spark: RPAD(s, n, pad) — right-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END
CREATE TABLE user_ids (
  user_id INT,
  username STRING,
  pad_char STRING,
  desired_len INT
);
