-- rule: grouping_sets
-- spark: GROUPING SETS((a,b),(a),(b),()) — multi-level grouping
-- feldera: GROUPING SETS — same syntax, supported in Feldera
CREATE OR REPLACE TEMP VIEW order_grouping_v3 AS SELECT country, category, SUM(revenue) as total_revenue, SUM(order_count) as total_orders FROM order_details_v3 GROUP BY GROUPING SETS((country, category), (country), (category), ());
