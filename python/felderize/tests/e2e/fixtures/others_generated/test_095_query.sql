CREATE OR REPLACE TEMP VIEW val104_nonmatching_name AS
WITH derived AS (
  SELECT row_id, nullif(first_name, last_name) AS nonmatching_name
  FROM scalar_function_rows
)
SELECT CASE WHEN nonmatching_name IS NULL THEN 'MISSING' ELSE 'PRESENT' END AS value_flag, COUNT(*) AS row_count
FROM derived
GROUP BY CASE WHEN nonmatching_name IS NULL THEN 'MISSING' ELSE 'PRESENT' END
HAVING COUNT(*) > 0;
