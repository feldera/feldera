-- rule: select_no_from
-- spark: SELECT expr GROUP BY ... HAVING ... with no FROM clause
-- feldera: Add dummy FROM: FROM (VALUES (1)) AS t(x)
CREATE OR REPLACE TEMP VIEW order_metrics_v3 AS SELECT 'ALL_ORDERS' AS metric_name, COUNT(*) AS order_count, MAX(CAST(5 AS INT)) AS max_priority GROUP BY 'ALL_ORDERS' HAVING COUNT(*) > 2;
