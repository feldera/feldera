-- rule: primary_key_constraint
-- spark: CONSTRAINT pk_name PRIMARY KEY (col) — named constraint syntax
-- feldera: PRIMARY KEY (col) — drop the CONSTRAINT name wrapper; also ensure all PK columns are NOT NULL
CREATE TABLE order_logs (
  order_id INT NOT NULL,
  customer_name STRING NOT NULL,
  order_timestamp TIMESTAMP,
  status STRING,
  CONSTRAINT pk_orders PRIMARY KEY (order_id)
);
