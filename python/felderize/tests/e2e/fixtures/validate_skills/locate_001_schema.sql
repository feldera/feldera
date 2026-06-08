-- rule: locate
-- spark: LOCATE(substr, str) — find 1-based position of substring
-- feldera: POSITION(substr IN str)
CREATE TABLE email_search (
  id INT,
  email_address STRING,
  domain_part STRING
);
