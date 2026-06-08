CREATE OR REPLACE TEMP VIEW audit_trail_v2 AS SELECT audit_id, action, user_id, timestamp_action, TIMESTAMP '2026-03-30 16:57:12' AS audit_recorded_at FROM audit_trail_t2 WHERE user_id > 10;
