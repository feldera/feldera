CREATE TABLE customer_records (
  customer_id BIGINT,
  full_name VARCHAR,
  account_code VARCHAR,
  signup_date VARCHAR,
  notes VARCHAR
);

CREATE VIEW formatted_customers AS
SELECT
  customer_id,
  INITCAP(full_name) AS display_name,
  CASE WHEN LENGTH(account_code) >= 10 THEN SUBSTRING(account_code, 1, 10)
       ELSE CONCAT(REPEAT('0', 10 - LENGTH(account_code)), account_code) END AS padded_code,
  CAST(signup_date AS DATE) AS signup_date,
  COALESCE(notes, 'No notes') AS notes,
  CONCAT_WS(' | ', full_name, account_code) AS search_key
FROM customer_records;
