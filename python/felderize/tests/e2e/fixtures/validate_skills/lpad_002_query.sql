-- rule: lpad
-- spark: LPAD(s, n, pad) — left-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END
CREATE OR REPLACE TEMP VIEW lpad_user_identifiers AS SELECT user_id, LPAD(identifier, desired_length, pad_char) AS formatted_id FROM user_ids;
