-- rule: lpad
-- spark: LPAD(s, n, pad) — left-pad string to width n
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END
CREATE TABLE transaction_refs (trans_id INT, ref_num STRING, min_length INT);
