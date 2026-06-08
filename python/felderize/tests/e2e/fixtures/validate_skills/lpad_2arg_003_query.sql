-- rule: lpad_2arg
-- spark: LPAD(s, n) — 2-arg form: left-pad to width n using space as default pad character
-- feldera: CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(' ', n-LENGTH(s)), s) END — use space literal in REPEAT since Feldera has no 2-arg LPAD
CREATE OR REPLACE TEMP VIEW padded_text AS SELECT sample_id, LPAD(text_value, 6) AS result FROM string_samples;
