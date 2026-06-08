-- rule: primary_key_constraint
-- spark: CONSTRAINT pk_name PRIMARY KEY (col) — named constraint syntax
-- feldera: PRIMARY KEY (col) — drop the CONSTRAINT name wrapper; also ensure all PK columns are NOT NULL
CREATE TABLE products_catalog (
  product_id BIGINT NOT NULL,
  sku STRING NOT NULL,
  name STRING,
  price DECIMAL(10, 2),
  CONSTRAINT pk_products PRIMARY KEY (product_id)
);
