-- rule: primary_key_constraint
-- spark: CONSTRAINT pk_name PRIMARY KEY (col) — named constraint syntax
-- feldera: PRIMARY KEY (col) — drop the CONSTRAINT name wrapper; also ensure all PK columns are NOT NULL
CREATE OR REPLACE TEMP VIEW order_logs_view AS SELECT order_id, customer_name, order_timestamp, status FROM order_logs WHERE status IS NOT NULL;
