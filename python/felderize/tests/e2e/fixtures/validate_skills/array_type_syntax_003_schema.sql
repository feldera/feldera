-- rule: array_type_syntax
-- spark: ARRAY<T> type syntax in DDL (e.g., ARRAY<STRING>, ARRAY<INT>)
-- feldera: T ARRAY suffix syntax — e.g. VARCHAR ARRAY, INT ARRAY. Never use ARRAY<T> in Feldera DDL
CREATE TABLE order_items (order_id INT, item_quantities ARRAY<DECIMAL(10,2)>, order_timestamp TIMESTAMP);
