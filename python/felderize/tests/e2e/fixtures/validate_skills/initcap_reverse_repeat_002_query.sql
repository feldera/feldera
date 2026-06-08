-- rule: initcap_reverse_repeat
-- spark: INITCAP(s) — capitalize first letter of each word; REVERSE(s) — reverse characters; REPEAT(s, n) — repeat string n times
-- feldera: INITCAP(s) / REVERSE(s) / REPEAT(s, n) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW message_view AS SELECT msg_id, REPEAT(text, 2) AS doubled FROM message_log WHERE LENGTH(text) > 0;
