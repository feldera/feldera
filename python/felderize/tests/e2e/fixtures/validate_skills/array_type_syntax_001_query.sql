-- rule: array_type_syntax
-- spark: ARRAY<T> type syntax in DDL (e.g., ARRAY<STRING>, ARRAY<INT>)
-- feldera: T ARRAY suffix syntax — e.g. VARCHAR ARRAY, INT ARRAY. Never use ARRAY<T> in Feldera DDL
CREATE OR REPLACE TEMP VIEW product_tags_view AS SELECT product_id, tag_names, created_at FROM product_tags WHERE SIZE(tag_names) > 1;
