-- rule: coalesce_func
-- spark: COALESCE(expr1, expr2, ...) — return first non-NULL value
-- feldera: COALESCE(expr1, expr2, ...) — works identically in Feldera, no translation needed
CREATE TABLE contact_details (
  contact_id INT,
  phone_primary STRING,
  phone_secondary STRING,
  phone_emergency STRING,
  status STRING
);
