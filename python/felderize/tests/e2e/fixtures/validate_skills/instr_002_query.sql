-- rule: instr
-- spark: instr(str, substr) — 1-based position of first occurrence (same as LOCATE but arg order reversed)
-- feldera: POSITION(substr IN str)
CREATE OR REPLACE TEMP VIEW email_domain_v2 AS SELECT user_id, email, instr(email, '@') AS at_sign_position FROM email_addresses;
