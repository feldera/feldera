-- rule: coalesce_func
-- spark: COALESCE(expr1, expr2, ...) — return first non-NULL value
-- feldera: COALESCE(expr1, expr2, ...) — works identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW contact_fallback AS SELECT contact_id, COALESCE(phone_primary, phone_secondary, phone_emergency, 'NO_CONTACT') AS reachable_phone, status FROM contact_details WHERE status IS NOT NULL;
