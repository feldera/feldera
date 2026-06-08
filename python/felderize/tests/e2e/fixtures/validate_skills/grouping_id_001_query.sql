-- rule: grouping_id
-- spark: grouping_id(col, ...) — bitmask identifying which columns are aggregated
-- feldera: grouping_id(col, ...) — same
CREATE OR REPLACE TEMP VIEW sales_rollup_v1 AS SELECT region, product, year, SUM(revenue) as total_revenue, grouping_id(region, product, year) as grouping_mask FROM sales_data_1 GROUP BY ROLLUP(region, product, year);
