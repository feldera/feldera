CREATE OR REPLACE TEMP VIEW event_log_v1 AS SELECT event_id, event_name, created_at, TIMESTAMP '2026-03-30 16:57:12' AS logged_at FROM event_log_t1;
