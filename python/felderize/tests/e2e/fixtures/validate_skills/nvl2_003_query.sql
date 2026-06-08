-- rule: nvl2
-- spark: NVL2(a, b, c) — return b if a is NOT NULL, else c
-- feldera: CASE WHEN a IS NOT NULL THEN b ELSE c END
CREATE OR REPLACE TEMP VIEW contact_method AS SELECT customer_id, NVL2(phone_number, phone_number, backup_phone) AS primary_contact FROM customer_contact;
