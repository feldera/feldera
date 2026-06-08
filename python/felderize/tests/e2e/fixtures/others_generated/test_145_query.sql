CREATE OR REPLACE TEMP VIEW gpt145_format_string AS
SELECT
  order_id,
  format_string('Order #%d for %s: %s ($%.2f)', order_id, customer_name, status, amount) AS summary
FROM order_events;
