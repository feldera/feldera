CREATE OR REPLACE TEMP VIEW bm11_exploded_items AS
SELECT
  se.user_id,
  element_at(se.attributes, 'campaign') AS campaign,
  item.sku AS sku,
  SUM(item.qty) AS total_qty,
  SUM(item.qty * item.price) AS gross_revenue
FROM session_events se
LATERAL VIEW explode(se.items) exploded_items AS item
GROUP BY
  se.user_id,
  element_at(se.attributes, 'campaign'),
  item.sku;
