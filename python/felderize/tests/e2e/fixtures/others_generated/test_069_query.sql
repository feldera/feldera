CREATE OR REPLACE TEMP VIEW val15_daily_sequence AS
SELECT row_id, seq_day
FROM collection_events
LATERAL VIEW explode(sequence(start_date, end_date, INTERVAL 1 DAY)) d AS seq_day;
