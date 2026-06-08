-- rule: primary_key_constraint
-- spark: CONSTRAINT pk_name PRIMARY KEY (col) — named constraint syntax
-- feldera: PRIMARY KEY (col) — drop the CONSTRAINT name wrapper; also ensure all PK columns are NOT NULL
CREATE OR REPLACE TEMP VIEW products_view AS SELECT product_id, sku, name, price FROM products_catalog WHERE price IS NOT NULL;
