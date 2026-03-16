CREATE OR REPLACE TEMP VIEW formatted_customers AS
SELECT
  customer_id,
  INITCAP(full_name) AS display_name,
  LPAD(account_code, 10, '0') AS padded_code,
  TO_DATE(signup_date, 'yyyy-MM-dd') AS signup_date,
  NVL(notes, 'No notes') AS notes,
  CONCAT_WS(' | ', full_name, account_code) AS search_key
FROM customer_records;
