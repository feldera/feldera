-- rule: lpad
-- spark: LPAD(s, n, pad) — left-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END
CREATE OR REPLACE TEMP VIEW lpad_formatted_refs AS SELECT trans_id, LPAD(ref_num, min_length, ' ') AS spaced_ref FROM transaction_refs;
