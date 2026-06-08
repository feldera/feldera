CREATE OR REPLACE TEMP VIEW bm45_regions_above_average AS
SELECT region, SUM(amount) AS total_amount FROM region_order_totals GROUP BY region
HAVING SUM(amount) > (SELECT AVG(amount) FROM region_order_totals);
