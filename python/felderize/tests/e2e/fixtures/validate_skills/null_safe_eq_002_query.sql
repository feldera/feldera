-- rule: null_safe_eq
-- spark: a <=> b — null-safe equality (true when both NULL)
-- feldera: a <=> b — same syntax, supported in Feldera
CREATE OR REPLACE TEMP VIEW order_return_details AS SELECT o.order_id, o.customer_id, o.status, r.reason FROM orders o LEFT JOIN returns r ON o.status <=> r.reason WHERE o.status <=> r.reason OR (o.status IS NULL AND r.reason IS NULL);
