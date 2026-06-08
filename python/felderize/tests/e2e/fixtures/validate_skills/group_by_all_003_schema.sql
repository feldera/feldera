-- rule: group_by_all
-- spark: GROUP BY ALL — automatically group by all non-aggregate expressions
-- feldera: Expand to explicit column list: GROUP BY col1, col2, ... (all non-aggregate SELECT expressions)
CREATE TABLE transaction_log_v3 (customer_id INT, country STRING, category STRING, amount DECIMAL(12,2));
