-- rule: grouping_id
-- spark: grouping_id(col, ...) — bitmask identifying which columns are aggregated
-- feldera: grouping_id(col, ...) — same
CREATE OR REPLACE TEMP VIEW order_summary_v3 AS SELECT customer_id, order_date, category, SUM(amount) as total_amount, grouping_id(customer_id, order_date, category) as grouping_mask FROM order_events_3 GROUP BY ROLLUP(customer_id, order_date, category);
