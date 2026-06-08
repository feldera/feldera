-- rule: md5_ascii_chr
-- spark: MD5(s) — MD5 hex digest of string; ASCII(s) — ASCII code of first character; CHR(n) — character from ASCII code
-- feldera: MD5(s) / ASCII(s) / CHR(n) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW msg_fingerprint_v3 AS SELECT msg_id, sender, MD5(msg_text) AS msg_digest, ASCII(sender) AS sender_first_ascii, CHR(90) AS last_letter FROM message_log WHERE LENGTH(msg_text) > 0;
