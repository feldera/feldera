-- rule: lateral_view_outer
-- spark: LATERAL VIEW OUTER explode(arr) t AS item — outer explode: preserves rows where array is NULL or empty (returns NULL for item column)
-- feldera: UNSUPPORTED — Feldera has no OUTER equivalent. UNNEST drops rows where array is NULL or empty. Mark as unsupported and add a warning comment.
CREATE OR REPLACE TEMP VIEW product_tags_exploded AS
SELECT
  product_id,
  product_name,
  tag
FROM product_tags
LATERAL VIEW OUTER explode(tags) t AS tag;
