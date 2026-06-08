-- rule: instr
-- spark: instr(str, substr) — 1-based position of first occurrence (same as LOCATE but arg order reversed)
-- feldera: POSITION(substr IN str)
CREATE TABLE email_addresses (user_id INT, email STRING);
