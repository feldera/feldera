-- rule: initcap_reverse_repeat
-- spark: INITCAP(s) — capitalize first letter of each word; REVERSE(s) — reverse characters; REPEAT(s, n) — repeat string n times
-- feldera: INITCAP(s) / REVERSE(s) / REPEAT(s, n) — all work identically in Feldera, no translation needed
CREATE TABLE message_log (msg_id INT, text STRING);
