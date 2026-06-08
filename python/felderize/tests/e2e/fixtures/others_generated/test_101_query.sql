CREATE OR REPLACE TEMP VIEW val127_title_name AS
WITH derived AS (
  SELECT grp, initcap(first_name) AS title_name
  FROM scalar_function_rows
)
SELECT title_name, COUNT(*) AS row_count
FROM derived
GROUP BY title_name
ORDER BY row_count DESC, title_name ASC
LIMIT 10;
