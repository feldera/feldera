CREATE OR REPLACE TEMP VIEW val141_parsed_date AS
SELECT row_id, to_date(cast(event_ts AS STRING), 'yyyy-MM-dd HH:mm:ss') AS parsed_date
FROM scalar_function_rows
WHERE event_date IS NOT NULL;
