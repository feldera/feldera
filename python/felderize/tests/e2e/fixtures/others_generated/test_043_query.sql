CREATE OR REPLACE TEMP VIEW bm60_regions_with_many_customers AS
SELECT region, COUNT(DISTINCT customer_id) AS distinct_customers FROM regional_customers GROUP BY region HAVING COUNT(DISTINCT customer_id) >= 3;
