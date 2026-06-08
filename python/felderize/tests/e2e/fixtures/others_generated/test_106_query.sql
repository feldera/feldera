CREATE OR REPLACE TEMP VIEW val147_month_start AS
WITH derived AS (
  SELECT grp, trunc(event_date, 'MM') AS month_start
  FROM scalar_function_rows
)
SELECT grp, COUNT(*) AS row_count
FROM derived
GROUP BY grp
HAVING COUNT(*) > 0;
