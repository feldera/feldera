CREATE OR REPLACE TEMP VIEW val148_synthetic_date AS
SELECT row_id, event_date, make_date(2026, month(event_date), day(event_date)) AS synthetic_date
FROM scalar_function_rows
ORDER BY synthetic_date DESC NULLS LAST, row_id ASC
LIMIT 12;
