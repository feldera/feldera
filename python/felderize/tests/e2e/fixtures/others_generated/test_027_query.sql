CREATE OR REPLACE TEMP VIEW bm39_customers_without_open_tickets AS
SELECT c.customer_id, c.customer_name FROM customer_accounts c
WHERE NOT EXISTS (
  SELECT 1 FROM support_cases s WHERE s.customer_id = c.customer_id AND s.case_status = 'OPEN'
);
