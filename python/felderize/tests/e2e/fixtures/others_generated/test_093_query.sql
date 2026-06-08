CREATE OR REPLACE TEMP VIEW val102_effective_discount AS
WITH derived AS (
  SELECT grp, ifnull(discount, amount) AS effective_discount
  FROM scalar_function_rows
  WHERE status <> 'DELETED'
)
SELECT grp, AVG(CAST(effective_discount AS DOUBLE)) AS avg_effective_discount
FROM derived
GROUP BY grp;
