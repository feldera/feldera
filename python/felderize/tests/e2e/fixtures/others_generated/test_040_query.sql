CREATE OR REPLACE TEMP VIEW bm56_supplier_price_stats AS
SELECT supplier_id, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price FROM supplier_prices GROUP BY supplier_id;
