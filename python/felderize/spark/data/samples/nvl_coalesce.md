---
categories: [string]
---

# LPAD / RPAD rewrite

Feldera does not support LPAD/RPAD natively. Rewrite using CASE WHEN + REPEAT + CONCAT.
Pad argument is optional in Spark (defaults to space); handle both 2-arg and 3-arg forms.

Spark:
```sql
SELECT
  lpad(account_id, 10, '0')   AS padded_id,
  rpad(product_code, 8, '-')  AS padded_code,
  lpad(label, 6)              AS space_padded
FROM products;
```

Feldera:
```sql
SELECT
  CASE WHEN LENGTH(account_id) >= 10 THEN SUBSTRING(account_id, 1, 10)
       ELSE CONCAT(REPEAT('0', 10 - LENGTH(account_id)), account_id) END  AS padded_id,
  CASE WHEN LENGTH(product_code) >= 8 THEN SUBSTRING(product_code, 1, 8)
       ELSE CONCAT(product_code, REPEAT('-', 8 - LENGTH(product_code))) END AS padded_code,
  CASE WHEN LENGTH(label) >= 6 THEN SUBSTRING(label, 1, 6)
       ELSE CONCAT(REPEAT(' ', 6 - LENGTH(label)), label) END              AS space_padded
FROM products;
```
