CREATE OR REPLACE TEMP VIEW bm63_inventory_grouping_sets AS
SELECT region, category, SUM(value) AS total_value
FROM inventory_value
GROUP BY GROUPING SETS ((region, category), (region))
HAVING SUM(value) > 0;
