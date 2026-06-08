-- rule: group_by_all
-- spark: GROUP BY ALL — automatically group by all non-aggregate expressions
-- feldera: Expand to explicit column list: GROUP BY col1, col2, ... (all non-aggregate SELECT expressions)
CREATE TABLE sales_data_v1 (region STRING, product STRING, quarter INT, revenue DECIMAL(10,2));
