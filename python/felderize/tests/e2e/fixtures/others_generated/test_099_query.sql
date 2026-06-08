CREATE OR REPLACE TEMP VIEW val124_left_padded_code AS
WITH derived AS (
  SELECT row_id, lpad(code, 8, '0') AS left_padded_code
  FROM scalar_function_rows
)
SELECT CASE WHEN left_padded_code IS NULL OR left_padded_code = '' THEN 'EMPTY' ELSE 'FILLED' END AS fill_state, COUNT(*) AS cnt
FROM derived
GROUP BY CASE WHEN left_padded_code IS NULL OR left_padded_code = '' THEN 'EMPTY' ELSE 'FILLED' END;
