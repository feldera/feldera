-- rule: rank_topk
-- spark: RANK() OVER (PARTITION BY ... ORDER BY ...) — same TopK restriction
-- feldera: RANK() same; must be in subquery with WHERE filter on result
CREATE OR REPLACE TEMP VIEW quarterly_top_products_v2 AS SELECT product_id, category, sales_amount, quarter FROM (SELECT product_id, category, sales_amount, quarter, RANK() OVER (PARTITION BY quarter ORDER BY sales_amount DESC) AS product_rank FROM product_sales_v2) sub WHERE sub.product_rank <= 3;
