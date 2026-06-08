CREATE OR REPLACE TEMP VIEW bm79_margin_ordering AS
SELECT product_id, revenue - cost AS margin FROM product_margin ORDER BY margin DESC, product_id ASC;
