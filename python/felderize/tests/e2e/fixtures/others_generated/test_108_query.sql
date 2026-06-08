CREATE OR REPLACE TEMP VIEW val149_event_epoch AS
WITH derived AS (
  SELECT row_id, unix_timestamp(cast(event_ts AS STRING), 'yyyy-MM-dd HH:mm:ss') AS event_epoch
  FROM scalar_function_rows
)
SELECT CASE WHEN event_epoch IS NULL THEN 'MISSING' ELSE 'PRESENT' END AS state, COUNT(*) AS cnt
FROM derived
GROUP BY CASE WHEN event_epoch IS NULL THEN 'MISSING' ELSE 'PRESENT' END;
