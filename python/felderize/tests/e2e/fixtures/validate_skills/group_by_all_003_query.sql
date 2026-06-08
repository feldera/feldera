-- rule: group_by_all
-- spark: GROUP BY ALL — automatically group by all non-aggregate expressions
-- feldera: Expand to explicit column list: GROUP BY col1, col2, ... (all non-aggregate SELECT expressions)
CREATE OR REPLACE TEMP VIEW transaction_summary_v3 AS SELECT customer_id, country, category, SUM(amount) as total_amount, MIN(amount) as min_amount, MAX(amount) as max_amount FROM transaction_log_v3 GROUP BY ALL;
