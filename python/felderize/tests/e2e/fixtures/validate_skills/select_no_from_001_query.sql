-- rule: select_no_from
-- spark: SELECT expr GROUP BY ... HAVING ... with no FROM clause
-- feldera: Add dummy FROM: FROM (VALUES (1)) AS t(x)
CREATE OR REPLACE TEMP VIEW revenue_summary_v1 AS SELECT 'TOTAL' AS summary, COUNT(*) AS transaction_count, SUM(CAST(100 AS DECIMAL(10,2))) AS total_revenue GROUP BY 'TOTAL' HAVING SUM(CAST(100 AS DECIMAL(10,2))) > CAST(50 AS DECIMAL(10,2));
