CREATE OR REPLACE TEMP VIEW val171_percentile_approx_amount AS
SELECT region, percentile_approx(amount, 0.5) AS median_amount
FROM (SELECT c.region, o.amount FROM customer_dim c JOIN order_fact o ON c.customer_id = o.customer_id) x
GROUP BY region;
