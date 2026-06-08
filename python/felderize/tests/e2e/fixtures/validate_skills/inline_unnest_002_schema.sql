-- rule: inline_unnest
-- spark: LATERAL VIEW inline(arr_of_structs) t AS f1, f2 — unnest array of structs
-- feldera: UNNEST(arr) AS t(f1, f2) — field names become output columns
CREATE TABLE event_logs (event_id INT, user_name STRING, actions ARRAY<STRUCT<action_type STRING, timestamp TIMESTAMP>>);
