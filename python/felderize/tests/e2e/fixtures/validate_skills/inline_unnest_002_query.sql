-- rule: inline_unnest
-- spark: LATERAL VIEW inline(arr_of_structs) t AS f1, f2 — unnest array of structs
-- feldera: UNNEST(arr) AS t(f1, f2) — field names become output columns
CREATE OR REPLACE TEMP VIEW expanded_actions_v2 AS SELECT event_id, user_name, action_type, timestamp FROM event_logs LATERAL VIEW inline(actions) act AS action_type, timestamp;
