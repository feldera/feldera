-- rule: rollup
-- spark: ROLLUP(a, b) — hierarchical grouping
-- feldera: ROLLUP(a, b) — same
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT region, product, SUM(amount) as total_sales FROM sales_data_v1 GROUP BY ROLLUP(region, product) ORDER BY region, product;
