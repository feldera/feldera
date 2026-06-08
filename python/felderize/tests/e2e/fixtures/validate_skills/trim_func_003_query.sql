-- rule: trim_func
-- spark: TRIM(s) — trim leading and trailing spaces from string
-- feldera: TRIM(s) — same function, works identically in Feldera; → [GBD-WHITESPACE]: only ASCII space (0x20) trimmed, not \t/\n
CREATE OR REPLACE TEMP VIEW processed_logs_v3 AS SELECT log_id, TRIM(message) AS msg_clean, timestamp_val FROM log_messages ORDER BY log_id;
