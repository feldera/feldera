-- rule: array_type_syntax
-- spark: ARRAY<T> type syntax in DDL (e.g., ARRAY<STRING>, ARRAY<INT>)
-- feldera: T ARRAY suffix syntax — e.g. VARCHAR ARRAY, INT ARRAY. Never use ARRAY<T> in Feldera DDL
CREATE TABLE product_tags (product_id INT, tag_names ARRAY<STRING>, created_at TIMESTAMP);
