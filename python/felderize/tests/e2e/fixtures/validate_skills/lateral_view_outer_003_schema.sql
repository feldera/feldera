-- rule: lateral_view_outer
-- spark: LATERAL VIEW OUTER explode(arr) t AS item — outer explode: preserves rows where array is NULL or empty (returns NULL for item column)
-- feldera: UNSUPPORTED — Feldera has no OUTER equivalent. UNNEST drops rows where array is NULL or empty. Mark as unsupported and add a warning comment.
CREATE TABLE order_items (
  order_id INT,
  customer_id INT,
  skus ARRAY<STRING>
);
