-- rule: rank_topk
-- spark: RANK() OVER (PARTITION BY ... ORDER BY ...) — same TopK restriction
-- feldera: RANK() same; must be in subquery with WHERE filter on result
CREATE TABLE product_sales_v2 (product_id INT, category STRING, sales_amount DECIMAL(10, 2), quarter INT);
