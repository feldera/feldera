-- rule: array_type_syntax
-- spark: ARRAY<T> type syntax in DDL (e.g., ARRAY<STRING>, ARRAY<INT>)
-- feldera: T ARRAY suffix syntax — e.g. VARCHAR ARRAY, INT ARRAY. Never use ARRAY<T> in Feldera DDL
CREATE OR REPLACE TEMP VIEW order_items_view AS SELECT order_id, item_quantities, order_timestamp FROM order_items WHERE item_quantities[0] > 50;
