CREATE OR REPLACE TEMP VIEW bm48_clean_product_names AS
SELECT product_id, UPPER(TRIM(REPLACE(product_name, '-', ' '))) AS normalized_name FROM raw_products;
