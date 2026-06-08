CREATE OR REPLACE TEMP VIEW val168_aggregated_payment_coverage AS
SELECT c.region, COUNT(DISTINCT o.order_id) AS orders, SUM(p.payment_amount) AS collected_amount
FROM customer_dim c
JOIN order_fact o ON c.customer_id = o.customer_id
LEFT JOIN payment_fact p ON o.order_id = p.order_id
GROUP BY c.region
HAVING COUNT(DISTINCT o.order_id) > 0;
