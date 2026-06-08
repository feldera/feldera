-- rule: grouping_sets
-- spark: GROUPING SETS((a,b),(a),(b),()) — multi-level grouping
-- feldera: GROUPING SETS — same syntax, supported in Feldera
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT region, product, SUM(amount) as total_sales, COUNT(*) as transaction_count FROM sales_data_v1 GROUP BY GROUPING SETS((region, product), (region), (product), ());
