CREATE OR REPLACE TEMP VIEW val154_event_quarter AS
WITH derived AS (
  SELECT row_id, quarter(event_date) AS event_quarter
  FROM scalar_function_rows
)
SELECT CASE WHEN event_quarter IS NULL THEN 'MISSING' ELSE 'PRESENT' END AS state, COUNT(*) AS cnt
FROM derived
GROUP BY CASE WHEN event_quarter IS NULL THEN 'MISSING' ELSE 'PRESENT' END;
