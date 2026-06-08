-- rule: locate
-- spark: LOCATE(substr, str) — find 1-based position of substring
-- feldera: POSITION(substr IN str)
CREATE OR REPLACE TEMP VIEW email_positions AS SELECT
  id,
  email_address,
  LOCATE('@', email_address) AS at_sign_pos,
  LOCATE('gmail', email_address) AS gmail_pos
FROM email_search;
