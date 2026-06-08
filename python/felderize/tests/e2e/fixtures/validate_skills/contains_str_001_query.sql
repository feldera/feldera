-- rule: contains_str
-- spark: contains(s, sub) — true if s contains sub
-- feldera: POSITION(sub IN s) > 0
CREATE OR REPLACE TEMP VIEW product_search_v1 AS SELECT product_id, description, contains(description, 'premium') AS is_premium FROM product_desc;
