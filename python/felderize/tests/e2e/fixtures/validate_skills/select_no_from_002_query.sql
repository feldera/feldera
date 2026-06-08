-- rule: select_no_from
-- spark: SELECT expr GROUP BY ... HAVING ... with no FROM clause
-- feldera: Add dummy FROM: FROM (VALUES (1)) AS t(x)
CREATE OR REPLACE TEMP VIEW shift_statistics_v2 AS SELECT 'WEEKLY' AS period, COUNT(*) AS total_shifts, AVG(CAST(8 AS DOUBLE)) AS avg_hours GROUP BY 'WEEKLY' HAVING COUNT(*) >= 3;
