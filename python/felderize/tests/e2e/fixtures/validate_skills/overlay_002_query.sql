-- rule: overlay
-- spark: overlay(str placing repl from pos for len) — replace substring
-- feldera: OVERLAY(str PLACING repl FROM pos FOR len)
CREATE OR REPLACE TEMP VIEW overlay_result_v2 AS SELECT feedback_id, overlay(message placing 'XXX' from 1 for 3) as anonymized_message FROM user_feedback_v2;
