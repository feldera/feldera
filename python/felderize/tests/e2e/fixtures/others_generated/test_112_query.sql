CREATE OR REPLACE TEMP VIEW val170_top_customers_by_amount AS
SELECT c.customer_id, c.region, SUM(o.amount) AS total_amount
FROM customer_dim c
JOIN order_fact o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.region
ORDER BY total_amount DESC, c.customer_id ASC
LIMIT 20;
