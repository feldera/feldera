CREATE OR REPLACE TEMP VIEW val27_full_name AS
SELECT row_id, concat_ws(' ', first_name, last_name) AS full_name
FROM text_date_events;
