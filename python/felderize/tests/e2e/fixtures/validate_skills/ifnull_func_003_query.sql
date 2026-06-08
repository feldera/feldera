-- rule: ifnull_func
-- spark: IFNULL(a, b) — return b if a is NULL, otherwise a; equivalent to COALESCE(a, b)
-- feldera: IFNULL(a, b) — same function, supported directly in Feldera; no translation needed
CREATE OR REPLACE TEMP VIEW contact_info AS SELECT customer_id, IFNULL(email, 'no-email@default.com') AS contact_email, IFNULL(phone, '0000000000') AS contact_phone, IFNULL(preferred_contact, 'email') AS method FROM customer_contact;
