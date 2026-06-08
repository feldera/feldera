-- rule: collect_list_distinct
-- spark: collect_list(distinct col) — aggregate distinct column values per group into an array (deduplicating)
-- feldera: ARRAY_DISTINCT(ARRAY_AGG(col)) — Feldera does not support collect_list(distinct col); use ARRAY_DISTINCT(ARRAY_AGG(col)) instead
CREATE TABLE product_sales (sale_id INT, category STRING, product_id INT);
CREATE TABLE product_sales_data AS SELECT * FROM product_sales WHERE 1=0;
