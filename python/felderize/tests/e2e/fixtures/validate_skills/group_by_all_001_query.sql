-- rule: group_by_all
-- spark: GROUP BY ALL — automatically group by all non-aggregate expressions
-- feldera: Expand to explicit column list: GROUP BY col1, col2, ... (all non-aggregate SELECT expressions)
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT region, product, quarter, SUM(revenue) as total_revenue FROM sales_data_v1 GROUP BY ALL;
