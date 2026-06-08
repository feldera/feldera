CREATE OR REPLACE TEMP VIEW val49_quarter_label AS
SELECT row_id, quarter(event_date) AS event_quarter
FROM text_date_events;
