-- rule: equal_null
-- spark: equal_null(a, b) — null-safe equality returning true when both args are NULL
-- feldera: a <=> b — same semantics, Feldera supports <=> operator
CREATE OR REPLACE TEMP VIEW product_match_v2 AS SELECT product_id, color, size, (color <=> size) AS attrs_match FROM product_attrs;
