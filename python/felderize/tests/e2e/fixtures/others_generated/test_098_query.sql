CREATE OR REPLACE TEMP VIEW val122_full_name AS
WITH derived AS (
  SELECT grp, concat_ws(' ', first_name, last_name) AS full_name
  FROM scalar_function_rows
)
SELECT full_name, COUNT(*) AS row_count
FROM derived
GROUP BY full_name
ORDER BY row_count DESC, full_name ASC
LIMIT 10;
