-- rule: struct_type
-- spark: STRUCT<a: T, b: U> type in DDL
-- feldera: ROW(a T, b U) — Feldera uses ROW with positional syntax
CREATE TABLE product_catalog (
  product_id INT,
  product_name STRING,
  specs STRUCT<color: STRING, weight_kg: DECIMAL(8,2), in_stock: BOOLEAN>
);
