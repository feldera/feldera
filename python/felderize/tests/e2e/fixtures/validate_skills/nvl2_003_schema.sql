-- rule: nvl2
-- spark: NVL2(a, b, c) — return b if a is NOT NULL, else c
-- feldera: CASE WHEN a IS NOT NULL THEN b ELSE c END
CREATE TABLE customer_contact (customer_id INT, phone_number VARCHAR(20), email VARCHAR(100), backup_phone VARCHAR(20));
