CREATE OR REPLACE TEMP VIEW val172_stddev_amount_by_segment AS
SELECT c.segment, stddev_samp(o.amount) AS stddev_amount
FROM customer_dim c JOIN order_fact o ON c.customer_id = o.customer_id
GROUP BY c.segment;
