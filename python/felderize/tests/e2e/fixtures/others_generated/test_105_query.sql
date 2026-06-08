CREATE OR REPLACE TEMP VIEW val142_parsed_ts AS
WITH derived AS (
  SELECT grp, to_timestamp(cast(event_ts AS STRING), 'yyyy-MM-dd HH:mm:ss') AS parsed_ts
  FROM scalar_function_rows
)
SELECT grp, COUNT(*) AS row_count
FROM derived
GROUP BY grp
HAVING COUNT(*) > 0;
