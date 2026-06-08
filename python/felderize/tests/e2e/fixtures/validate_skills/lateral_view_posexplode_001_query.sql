-- rule: lateral_view_posexplode
-- spark: LATERAL VIEW posexplode(arr) t AS pos, item — unnest with 0-based position index
-- feldera: UNNEST(arr) WITH ORDINALITY AS t(item, pos) — Feldera ordinal is 1-based, comes after value
CREATE OR REPLACE TEMP VIEW product_tags_exploded AS SELECT product_id, pos, item FROM product_tags LATERAL VIEW posexplode(tags) t AS pos, item;
