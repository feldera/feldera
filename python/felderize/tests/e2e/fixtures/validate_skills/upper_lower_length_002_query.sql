-- rule: upper_lower_length
-- spark: UPPER(s), LOWER(s), LENGTH(s), SUBSTRING(s, pos, len) — basic string functions
-- feldera: UPPER(s) / LOWER(s) / LENGTH(s) / SUBSTRING(s, pos, len) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW message_view AS SELECT user_id, SUBSTRING(UPPER(message), 1, 10) AS msg_start, LENGTH(subject) AS subject_length, LOWER(subject) AS lower_subject FROM user_messages WHERE LENGTH(subject) >= 5;
