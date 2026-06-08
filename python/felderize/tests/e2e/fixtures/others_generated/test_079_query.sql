CREATE OR REPLACE TEMP VIEW val72_top_regions_by_avg AS
SELECT region, AVG(amount) AS avg_amount
FROM order_fact
GROUP BY region
HAVING AVG(amount) > 0
ORDER BY avg_amount DESC
LIMIT 5;
