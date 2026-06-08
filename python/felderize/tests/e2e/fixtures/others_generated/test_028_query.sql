CREATE OR REPLACE TEMP VIEW bm40_products_above_category_peak AS
SELECT c1.category, c1.product_id, c1.price FROM category_prices c1
WHERE c1.price = (SELECT MAX(c2.price) FROM category_prices c2 WHERE c2.category = c1.category);
